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
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	v, err := r.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}

	res := kvrpcpb.RawGetResponse{
		Value:    v,
		NotFound: v == nil,
	}

	return &res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	if err := server.storage.Write(nil, []storage.Modify{{Data: storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.Cf,
	}}}); err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	if err := server.storage.Write(nil, []storage.Modify{{Data: storage.Delete{
		Key: req.GetKey(),
		Cf:  req.Cf,
	}}}); err != nil {
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	iter := r.IterCF(req.Cf)
	defer iter.Close()

	iter.Seek(req.StartKey)

	var kvs []*kvrpcpb.KvPair
	for i := uint32(0); i < req.Limit; i++ {
		if !iter.Valid() {
			break
		}

		item := iter.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			return nil, err
		}

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})

		iter.Next()
	}

	res := kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}

	return &res, nil
}
