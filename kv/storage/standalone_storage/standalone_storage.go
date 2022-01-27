package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := conf.DBPath + "/kv"
	raftPath := conf.DBPath + "/raft"
	kvEngine := engine_util.CreateDB(kvPath, false)
	var raftEngine *badger.DB
	if conf.Raft {
		raftEngine = engine_util.CreateDB(raftPath, true)
	}
	engine := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)

	return &StandAloneStorage{
		engine: engine,
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandaloneReaderImp{
		engine: s.engine,
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if batch == nil || len(batch) <= 0 {
		return nil
	}
	writeBatch := new(engine_util.WriteBatch)
	for _, modify := range batch {
		writeBatch.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	return s.engine.WriteKV(writeBatch)
}

// -------------------------------------

type StandaloneReaderImp struct {
	engine *engine_util.Engines
	txn    *badger.Txn
	iter   engine_util.DBIterator
}

func (imp *StandaloneReaderImp) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCF(imp.engine.Kv, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return value, err
}

func (imp *StandaloneReaderImp) IterCF(cf string) engine_util.DBIterator {
	if imp.txn != nil {
		imp.Close()
	}
	imp.txn = imp.engine.Kv.NewTransaction(false)
	imp.iter = engine_util.NewCFIterator(cf, imp.txn)
	return imp.iter
}

func (imp *StandaloneReaderImp) Close() {
	if imp.txn == nil {
		return
	}
	imp.iter.Close()
	imp.txn.Discard()
}
