package etcd

import (
	"context"
	"errors"

	v3 "go.etcd.io/etcd/clientv3"
	spb "go.etcd.io/etcd/mvcc/mvccpb"
)

var (
	ErrKeyExists      = errors.New("key already exists")
	ErrWaitMismatch   = errors.New("unexpected wait result")
	ErrTooManyClients = errors.New("too many clients")
	ErrNoWatcher      = errors.New("no watcher channel")
)

// deleteRevKey deletes a key by revision, returning false if key is missing
func deleteRevKey(kv v3.KV, key string, rev int64) (bool, error) {
	cmp := v3.Compare(v3.ModRevision(key), "=", rev)
	req := v3.OpDelete(key)
	txnresp, err := kv.Txn(context.TODO()).If(cmp).Then(req).Commit()
	if err != nil {
		return false, err
	} else if !txnresp.Succeeded {
		return false, nil
	}
	return true, nil
}

func claimFirstKey(kv v3.KV, kvs []*spb.KeyValue) (*spb.KeyValue, error) {
	for _, k := range kvs {
		ok, err := deleteRevKey(kv, string(k.Key), k.ModRevision)
		if err != nil {
			return nil, err
		} else if ok {
			return k, nil
		}
	}
	return nil, nil
}
