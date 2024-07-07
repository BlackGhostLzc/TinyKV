package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	// 拿到了一个 reader 之后
	value, err := reader.GetCF(req.GetCf(), req.GetKey())

	if err != nil {
		return &kvrpcpb.RawGetResponse{}, err
	}

	notFound := false
	if value == nil {
		notFound = true
	}

	response := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: notFound,
	}

	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}

	mod := storage.Modify{
		Data: put,
	}

	batch := []storage.Modify{mod}

	err := server.storage.Write(nil, batch)

	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	// 也是 Write 函数
	delete := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}

	mod := storage.Modify{
		Data: delete,
	}

	batch := []storage.Modify{mod}

	err := server.storage.Write(nil, batch)
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	// 先要得到一个 reader
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}

	dbIter := reader.IterCF(req.GetCf())
	var kvs []*kvrpcpb.KvPair
	cnt := uint32(0)
	for dbIter.Seek(req.StartKey); dbIter.Valid(); dbIter.Next() {
		item := dbIter.Item()
		val, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		cnt++
		if cnt == req.Limit {
			break
		}
	}
	// 返回响应
	resp := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	return resp, nil
}
