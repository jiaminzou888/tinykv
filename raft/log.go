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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// all entries that have not yet compact.
	entries []pb.Entry

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(fmt.Sprintf("raft newLog FirsIndex err:%s", err))
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(fmt.Sprintf("raft newLog LastIndex err:%s", err))
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(fmt.Sprintf("raft newLog Entries err:%s", err))
	}
	return &RaftLog{
		storage: storage,
		entries: entries,
		stabled: lastIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ents := []pb.Entry{}
	for _, ent := range l.entries {
		if ent.Index > l.stabled {
			ents = append(ents, ent)
		}
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents = make([]pb.Entry, 0, 5)
	for _, ent := range l.entries {
		if ent.Index > l.applied && ent.Index <= l.committed {
			ents = append(ents, ent)
		}
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var snapLast uint64
	// pendingSnapshot表示接下来要apply的快照
	if !IsEmptySnap(l.pendingSnapshot) {
		snapLast = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, snapLast)
	}
	// 因为在2b中，storage index初始化中无entry，但initIndex是5
	index, _ := l.storage.LastIndex()
	return max(index, snapLast)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		for _, ent := range l.entries {
			if ent.Index == i {
				return ent.Term, nil
			}
		}
	}
	// 因为在2b中，storage term初始化中无entry，但initTerm是5
	term, err := l.storage.Term(i)
	if err != nil && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
	}
	return term, err
}
