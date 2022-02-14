// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// Your Data Here (2A).
	voteCount           int
	rejectCount         int
	realElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	rand.Seed(time.Now().UnixNano())

	raft := Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		msgs:             []pb.Message{},
		State:            StateFollower,
		votes:            map[uint64]bool{},
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}

	hardState, confState, err := raft.RaftLog.storage.InitialState()
	if err != nil {
		panic(fmt.Sprintf("raft newRaft InitialState err:%s", err))
	}
	if c.peers == nil {
		c.peers = confState.Nodes
	}

	raft.Vote = hardState.Vote
	raft.Term = hardState.Term
	raft.RaftLog.committed = hardState.Commit

	lastIndex := raft.RaftLog.LastIndex()
	for _, id := range c.peers {
		// votes
		if id == raft.Vote {
			raft.votes[id] = true
		} else {
			raft.votes[id] = false
		}
		// Prs
		if id == raft.id {
			raft.Prs[id] = &Progress{
				Next:  lastIndex + 1,
				Match: lastIndex,
			}
		} else {
			raft.Prs[id] = &Progress{
				Next: lastIndex + 1,
			}
		}
	}
	raft.realElectionTimeout = raft.electionTimeout + rand.Intn(raft.electionTimeout)

	return &raft
}

// sendAppend sends a append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if to == r.id {
		return true
	}

	process := r.Prs[to]
	if process.Next == 0 && process.Match == 0 {
		process = &Progress{
			Next:  r.RaftLog.committed + 1,
			Match: r.RaftLog.committed,
		}
		r.Prs[to] = process
	}

	totalNextEntries := []*pb.Entry{}

	startNextIdx := len(r.RaftLog.entries)
	// 整个循环共用i和ent变量，并非一次循环新建一个，有坑
	for i, ent := range r.RaftLog.entries {
		if ent.Index == process.Next {
			startNextIdx = i
		}
		tmp := ent
		if i >= startNextIdx {
			totalNextEntries = append(totalNextEntries, &tmp)
		}
	}

	var prevEntry pb.Entry
	if startNextIdx > 0 {
		prevEntry = r.RaftLog.entries[startNextIdx-1]
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prevEntry.Index,
		LogTerm: prevEntry.Term,
		Entries: totalNextEntries,
		Commit:  r.RaftLog.committed,
	})

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: 0,
		Index:   0,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			for k, _ := range r.Prs {
				if k != r.id {
					r.sendHeartbeat(k)
				}
			}
			r.heartbeatElapsed = 0
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.realElectionTimeout {
			r.startElection(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
				Term:    r.Term,
			})
			r.electionElapsed = 0
			r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.resetVars()

	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.resetVars()

	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader

	lastIndex := r.RaftLog.LastIndex()
	for id, process := range r.Prs {
		if id == r.id {
			process.Match = lastIndex
		} else {
			process.Match = 0
		}
		process.Next = lastIndex + 1
	}

	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{
			{
				EntryType: pb.EntryType_EntryNormal,
				Data:      nil,
			},
		},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.startElection(m)
		case pb.MessageType_MsgPropose:
			fmt.Printf("follower id:%d propose:%s.\n", r.id, m.String())
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			fmt.Printf("follower id:%d append response:%s.\n", r.id, m.String())
		case pb.MessageType_MsgRequestVote:
			r.responseVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			fmt.Printf("follower id:%d get voteResponse from:%d which is reverted to before.\n", r.id, m.From)
		case pb.MessageType_MsgBeat:
			r.triggerHeartbeat(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			fmt.Printf("follower id:%d get heartbeatResponse from:%d which is reverted to before.\n", r.id, m.From)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.startElection(m)
		case pb.MessageType_MsgPropose:
			fmt.Printf("candidate id:%d propose: %s, what happend?\n", r.id, m.String())
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			fmt.Printf("candidate id:%d append response:%s, what happend?\n", r.id, m.String())
		case pb.MessageType_MsgRequestVote:
			r.responseVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.processElection(m)
		case pb.MessageType_MsgBeat:
			r.triggerHeartbeat(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			fmt.Printf("candidate id:%d get heartbeatResponse from:%d, what happend?\n", r.id, m.From)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.startElection(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.responseVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.processElection(m)
		case pb.MessageType_MsgBeat:
			r.triggerHeartbeat(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			for peer, _ := range r.Prs {
				r.sendAppend(peer)
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.MsgType != pb.MessageType_MsgAppend {
		return
	}

	if r.Term < m.Term || r.Lead == 0 {
		r.becomeFollower(m.Term, m.From)
	}

	reject := false

	for {
		// Log Match Property：比较任期，上条日志存在
		if r.Term > m.Term {
			reject = true
			break
		}

		logTerm, _ := r.RaftLog.Term(m.Index)
		if m.LogTerm != logTerm {
			reject = true
			break
		}

		// 0. 没有附带日志entry，仅仅是Commit消息
		if len(m.Entries) <= 0 {
			break
		}
		// 1. 当前follower暂无数据，直接追加日志数据
		if len(r.RaftLog.entries) <= 0 {
			for _, ent := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
			}
			break
		}
		// 2. 当前follower有数据，但匹配位置不同
		if logTerm == 0 && m.Index == 0 {
			// 2.1 匹配上空位置，查看后续位置是否匹配
			idx := uint64(0)
			msgLen := uint64(len(m.Entries))
			logLen := uint64(len(r.RaftLog.entries))
			for idx = 0; idx < msgLen && idx < logLen; idx++ {
				if m.Entries[idx].Index != r.RaftLog.entries[idx].Index ||
					m.Entries[idx].Term != r.RaftLog.entries[idx].Term {
					break
				}
			}
			if idx == msgLen {
				// 匹配所有msg ent，啥都不干
			} else if idx == logLen {
				// 匹配所有log ent，往后追加
				for ; idx < msgLen; idx++ {
					r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[idx])
				}
			} else {
				// 中途不匹配，先清理，再追加
				if idx == uint64(0) {
					r.RaftLog.stabled = 0
					r.RaftLog.entries = []pb.Entry{}
					for _, ent := range m.Entries {
						r.RaftLog.entries = append(r.RaftLog.entries, *ent)
					}
				} else {
					r.RaftLog.entries = r.RaftLog.entries[:idx]
					if r.RaftLog.stabled > r.RaftLog.entries[idx-1].Index {
						r.RaftLog.stabled = r.RaftLog.entries[idx-1].Index
					}
				}
			}
		} else {
			// 2.2 匹配在其他位置，先清理原数据，再用msgEntries数据
			idx := uint64(len(r.RaftLog.entries) - 1)
			for i, ent := range r.RaftLog.entries {
				if ent.Term == m.LogTerm && ent.Index == m.Index {
					idx = uint64(i)
					break
				}
			}
			r.RaftLog.entries = r.RaftLog.entries[:idx+1]
			if r.RaftLog.stabled > r.RaftLog.entries[idx].Index {
				r.RaftLog.stabled = r.RaftLog.entries[idx].Index
			}
			for _, ent := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
			}
		}
		break
	}

	// 只有有效的append才更新commit
	if !reject && r.Lead == m.From && m.Commit > r.RaftLog.committed {
		lastNewEntryIndex := m.Index
		if len(m.Entries) > 0 {
			lastNewEntryIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntryIndex)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    m.To,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	reject := false
	if r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
		r.Lead = m.From
		r.resetVars()
	} else {
		reject = true
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    m.To,
		To:      m.From,
		Term:    m.Term,
		Reject:  reject,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// -----------------------------------------------------------

func (r *Raft) startElection(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgHup {
		return
	}

	// 根据aa和ab的用例，如果Leader收到MsgHup需要执行选举，但Term不变
	if r.State == StateLeader {
		r.State = StateCandidate
	} else {
		r.becomeCandidate()
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	for id, _ := range r.Prs {
		if id == r.id {
			r.voteCount++
			r.votes[r.id] = true
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			})
		}
	}

	// 特殊情况，只有一台机器
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) processElection(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgRequestVoteResponse {
		return
	}

	if m.Reject {
		r.votes[m.From] = false
		r.rejectCount++
	} else {
		r.votes[m.From] = true
		r.voteCount++
	}

	if r.voteCount > (len(r.Prs) / 2) {
		if r.State != StateLeader {
			r.becomeLeader()
		}
	} else if r.rejectCount > (len(r.Prs) / 2) {
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
}

func (r *Raft) responseVote(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgRequestVote {
		return
	}

	// 安全性：拒绝投票的3种情况
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    m.To,
			To:      m.From,
			Term:    m.Term,
			Reject:  true,
		})
		return
	} else {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)

		if lastTerm != m.LogTerm {
			if lastTerm > m.LogTerm {
				if m.Term > r.Term {
					r.becomeFollower(m.Term, None)
				}
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    m.To,
					To:      m.From,
					Term:    m.Term,
					Reject:  true,
				})
				return
			}
		} else {
			if lastIndex > m.Index {
				if m.Term > r.Term {
					r.becomeFollower(m.Term, None)
				}
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    m.To,
					To:      m.From,
					Term:    m.Term,
					Reject:  true,
				})
				return
			}
		}
	}

	// 安全性满足，任期相同，如果本身是候选者或已投过票，拒绝
	if m.Term == r.Term {
		if r.State == StateCandidate || (r.Vote != None && r.Vote != m.From) {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    m.To,
				To:      m.From,
				Term:    m.Term,
				Reject:  true,
			})
			return
		}
	}

	// 选择策略：一轮选举只投一次票：先来先投
	r.becomeFollower(m.Term, None)

	r.Vote = m.From
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    m.To,
		To:      m.From,
		Term:    m.Term,
		Reject:  false,
	})
}

func (r *Raft) triggerHeartbeat(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgBeat {
		return
	}

	if r.State != StateLeader {
		return
	}

	for id, _ := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgPropose {
		return
	}

	if r.State != StateLeader {
		return
	}

	lastIndex := r.RaftLog.LastIndex()

	ents := []*pb.Entry{}
	for _, ent := range m.Entries {
		index := uint64(0)
		if len(ents) == 0 {
			index = lastIndex + 1
		} else {
			index = ents[len(ents)-1].Index
		}
		ents = append(ents, &pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      r.Term,
			Index:     index,
			Data:      ent.Data,
		})
	}

	for _, ent := range ents {
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	}

	for id := range r.Prs {
		if id == r.id {
			r.Prs[id].Match = lastIndex + uint64(len(ents))
			r.Prs[id].Next = r.Prs[id].Match + 1
		} else {
			r.sendAppend(id)
		}
	}

	// 只有一台机器，直接commit
	if len(r.Prs) == 1 {
		r.RaftLog.committed++
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgAppendResponse {
		return
	}

	if r.State != StateLeader {
		return
	}

	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}

	// 更新
	commitChange := false
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	// 只要有正常的append response，commit就尽力往后追
	for r.RaftLog.committed < r.RaftLog.LastIndex() {
		commitNum := 1 // 从自己开始
		nextCommit := r.RaftLog.committed + 1
		for id, process := range r.Prs {
			if r.id == id {
				continue
			}
			// 5.4.2: 只提交当前Term的日志
			if process.Match >= nextCommit {
				if matchTerm, _ := r.RaftLog.Term(process.Match); matchTerm == r.Term {
					commitNum++
				}
			}
		}
		if commitNum <= (len(r.Prs) / 2) {
			// 还不够？等其他机器的下一条message
			break
		} else {
			// 递增commit下标，继续检查。因为每台机器应答的顺序都不定，每次都需要找到大家公认的最远的commit
			r.RaftLog.committed++
			commitChange = true
		}
	}

	// committed有新的更新，通知其他节点准备应用
	if commitChange {
		for peer, _ := range r.Prs {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) resetVars() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	r.voteCount = 0
	r.rejectCount = 0

	r.Vote = None
	for id, _ := range r.votes {
		r.votes[id] = false
	}
}
