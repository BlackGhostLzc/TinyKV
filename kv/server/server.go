package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
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
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	kvPairs := make([]*kvrpcpb.KvPair, 0)
	for i := 0; i < int(req.Limit); i++ {
		if !scanner.Iter.Valid() {
			break
		}
		key, value, err := scanner.Next()

		if err != nil {
			return resp, err
		}
		if key == nil {
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		// 这里不能够读取，否则可能存在不可重复读的问题,当前正在有事务尝试对这个 key 作出更改
		// 假如 lock.Ts < txn.startTs 并且 lock.commitTs > txn.startTs，这样会存在不可重复读
		if lock != nil && lock.Ts < txn.StartTS {
			pair := &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
			}
			kvPairs = append(kvPairs, pair)
			continue
		}
		if value != nil {
			pair := &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			}
			kvPairs = append(kvPairs, pair)
		}
	}
	resp.Pairs = kvPairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	// 只有一个 primary key
	var keys [][]byte
	keys = append(keys, req.PrimaryKey)
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// 看一下这个事务有没有自行回滚
	write, _, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if write != nil {
		// rolled back: lock_ttl == 0 && commit_version == 0(resp的返回值)
		if write.Kind == mvcc.WriteKindRollback {
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
		// 否则应该是被提交了 committed: commit_version > 0
		resp.CommitVersion = write.StartTS
		return resp, nil
	}

	// 获取 primary key 的 lock
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if lock != nil {
		// 判断 lock 有没有过期
		lockTs := mvcc.PhysicalTime(lock.Ts)
		currentTs := mvcc.PhysicalTime(req.CurrentTs)
		if currentTs > lockTs && currentTs-lockTs >= lock.Ttl {
			// 过期了就要对 primary key 进行回滚
			txn.DeleteValue(req.PrimaryKey)
			txn.DeleteLock(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})

			err = server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return resp, err
			}
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			return resp, nil
		} else {
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
	}

	// 既没有 write,也没有 lock, 根据测试也要给一个 rollback write
	// 这种情况是什么原因引起的呢？
	// 因为 prewrite 过程是 primary key 和 secondary key 一起发送的，如果 prewrite primary key 丢失了
	// 丢失的时间甚至比 secondary key 的 lock ttl 还要长，那么就有可能出现这种情况
	// 这种情况下，primary key 所在的 tinykv 感知不到该事务的存在，而 secondary key 所在的 tinykv 知道事物的存在
	txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
		StartTS: req.LockTs,
		Kind:    mvcc.WriteKindRollback,
	})

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	resp.Action = kvrpcpb.Action_LockNotExistRollback

	return resp, nil
}

// KvBatchRollback 调用的时机
// 当事务想要获取某个 key 的锁的时候，发现已经存在锁了
// 并且这把锁已经过期了
// 那么判断这个做更改的事务有没有提交(KvCheckTxnStatus), 如果没有提交那就进行回滚
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	var keys [][]byte
	for _, key := range req.Keys {
		keys = append(keys, key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	resp := &kvrpcpb.BatchRollbackResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// 首先判断有没有提交，如果只要有1个 key 已经提交了，那就 fail
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}

		// 说明已经成功提交了
		// Will fail if the transaction has already been committed
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return resp, nil
		}
	}

	// 检查锁，看有没有被其他事务给上锁了
	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}

		// 判断有没有进行回滚
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		// 已经被回滚了(两个事务 T2 和 T3 都想要锁 key, key被 T1 锁住了, T2 率先进行 checkTxnStatus 检查
		// 并发现 key 的 primary 需要进行回滚, 然后对 key 也完成了回滚, 就出现了下面已经回滚的情况)
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}
		if lock != nil {
			if lock.Ts != txn.StartTS {
				// 说明被其他事务把锁给占据了, 也要进行回滚
				txn.PutWrite(key, txn.StartTS, &mvcc.Write{
					StartTS: txn.StartTS,
					Kind:    mvcc.WriteKindRollback,
				})
				continue
			}
			// 否则这把锁就是这个事务持有,
			txn.DeleteValue(key)
			txn.DeleteLock(key)
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}

		// lock 为空，需要打一个标记
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, nil
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := reader.IterCF(engine_util.CfLock)

	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == txn.StartTS {
			keys = append(keys, key)
		}
	}

	if req.CommitVersion == 0 {
		// rollback all
		rbReq := &kvrpcpb.BatchRollbackRequest{
			Keys:         keys,
			StartVersion: txn.StartTS,
			Context:      req.Context,
		}
		rbResp, err := server.KvBatchRollback(nil, rbReq)
		if err != nil {
			return resp, err
		}
		resp.Error = rbResp.Error
		resp.RegionError = rbResp.RegionError
		return resp, nil
	} else if req.CommitVersion > 0 {
		// commit those locks with the given commit timestamp
		cmReq := &kvrpcpb.CommitRequest{
			Keys:          keys,
			StartVersion:  txn.StartTS,
			CommitVersion: req.CommitVersion,
			Context:       req.Context,
		}
		cmResp, err := server.KvCommit(nil, cmReq)
		if err != nil {
			return resp, err
		}
		resp.Error = cmResp.Error
		resp.RegionError = cmResp.RegionError
		return resp, nil
	}

	return resp, nil
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
