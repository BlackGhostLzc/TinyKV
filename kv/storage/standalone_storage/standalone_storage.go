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
	config *config.Config // NewStandAloneStorage(conf *config.Config)构造函数
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	path := conf.DBPath
	kvPath := path + "/kv"
	raftPath := path + "/raft"

	kvEngine := engine_util.CreateDB(kvPath, false)
	raftEngine := engine_util.CreateDB(raftPath, false)
	standaloneStorage := &StandAloneStorage{
		config: conf,
		engine: engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath),
	}

	return standaloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engine.Close()
	return nil
}

// StorageReader 需要实现一个StorageReader的接口
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 返回一个 storage.StorageReader
	kvTxn := s.engine.Kv.NewTransaction(false)
	reader := &StandAloneStorageReader{
		txn: kvTxn,
	}
	return reader, nil
}

// func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error
// func DeleteCF(engine *badger.DB, cf string, key []byte) error
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, tmp := range batch {
		switch b := tmp.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engine.Kv, b.Cf, b.Key, b.Value)
			if err != nil {
				return err
			}
			break
		case storage.Delete:
			err := engine_util.DeleteCF(s.engine.Kv, b.Cf, b.Key)
			if err != nil {
				return err
			}
			break
		}
	}
	return nil
}

// 这里需要实现 GetCF(), IterCF(), Close()三个接口
func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil // 测试要求 err 为 nil，而不是 KeyNotFound，否则没法过
	}
	return value, err
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.txn.Discard()
}
