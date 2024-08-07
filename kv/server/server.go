package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	keys = append(keys, req.Key)
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.GetResponse{}
	// 建立事务
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}

	// 这把锁存在,并且锁的时间小于事务的开始时间,这就是写入冲突
	if lock != nil && lock.Ts < txn.StartTS {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}

	// 然后去找
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
		return resp, err
	}
	resp.Value = value

	return resp, nil
}

// KvPrewrite 1. 有 lock 不能写
// 2. 有 write 并且 write的提交时间大于事务的开始时间，也不能写
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	mutation := req.Mutations
	for _, mu := range mutation {
		keys = append(keys, mu.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range keys {
		// 检查该 key 是否满足上面两条
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil {
			// 有锁，写冲突
			keyError := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
			resp.Errors = append(resp.Errors, keyError)
			return resp, nil
		}
	}

	// 再检查是否有 write 冲突
	for _, key := range keys {
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		// write的提交时间大于该事务的开始时间
		if write != nil && commitTs > txn.StartTS {
			keyError := &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    write.StartTS,
					ConflictTs: commitTs,
					Key:        key,
					Primary:    req.PrimaryLock,
				},
			}
			resp.Errors = append(resp.Errors, keyError)
			return resp, nil
		}

	}

	// 接下来开始写 lock, 写 default
	for _, m := range mutation {
		var kind mvcc.WriteKind
		switch m.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(m.Key, m.Value)
			kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(m.Key)
			kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			kind = mvcc.WriteKindRollback
		}
		txn.PutLock(m.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

// KvCommit 把 lock 解锁， 添加新的 write 列
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	for _, key := range req.Keys {
		keys = append(keys, key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.CommitResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// 检查每一个 key 的 lock 是否存在
	// 如果存在，lock.startTs 是否等于 事务的开始时间
	// 上面这种的原因是，可能 TTL 到期了，从而被回滚了
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil {
			// 如果锁不存在了，那说明可能被 rollback 了
			write, _, err := txn.MostRecentWrite(key)
			if err != nil {
				return resp, err
			}
			if write != nil && write.StartTS == txn.StartTS {
				if write.Kind == mvcc.WriteKindRollback {
					resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
					return resp, nil
				}
			}
			continue
		}

		// 上锁失败，也要告诉客户端进行重试
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			return resp, nil
		}

		// 开始清除锁，写 write
		txn.DeleteLock(key)
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
