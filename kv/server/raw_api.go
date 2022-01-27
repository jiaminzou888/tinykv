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
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := make([]storage.Modify, 0, 1)
	batch = append(batch, storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	})

	if err := server.storage.Write(&kvrpcpb.Context{}, batch); err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := make([]storage.Modify, 0, 1)
	batch = append(batch, storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	})

	if err := server.storage.Write(&kvrpcpb.Context{}, batch); err != nil {
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}

	rsp := &kvrpcpb.RawScanResponse{
		Kvs: make([]*kvrpcpb.KvPair, 0, 1),
	}

	count := uint32(0)
	iter := reader.IterCF(req.Cf)
	for count < req.Limit && iter.Valid() {
		item := iter.Item()

		key := item.Key()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}

		rsp.Kvs = append(rsp.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
	}

	reader.Close()

	return rsp, nil
}
