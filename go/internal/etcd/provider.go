package etcd

import (
	"context"
	"errors"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type IEtcdProvider interface {
	Get(ctx context.Context, key string) (string, error)
	Put(ctx context.Context, key string, value string) error
	PutWithLease(ctx context.Context, key string, value string) error
	WatchByPrefix(ctx context.Context, prefix string) (chan Node, error)
	Lease(ctx context.Context, key string, ttl int64) (bool, error)
	Resign(ctx context.Context) error
}

type Node struct {
	Key   string
	Value string
}

type EtcdProvider struct {
	client  *clientv3.Client
	leaseID clientv3.LeaseID
}

func (p *EtcdProvider) snapshotByPrefix(ctx context.Context, prefix string, ch chan Node) (int64, error) {
	const pageSize int64 = 100
	startKey := prefix
	endKey := clientv3.GetPrefixRangeEnd(prefix)
	more := true
	rev := int64(0)
	for more {
		opts := []clientv3.OpOption{
			clientv3.WithRange(endKey),
			clientv3.WithLimit(pageSize),
			clientv3.WithRev(rev), // Will be 0 on first pass, then pinned
		}

		resp, err := p.client.Get(ctx, startKey, opts...)
		if err != nil {
			panic(err)
		}

		for _, kv := range resp.Kvs {
			node := Node{Key: string(kv.Key), Value: string(kv.Value)}
			select {
			case ch <- node:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}

		if rev == 0 {
			rev = resp.Header.Revision
		}

		if !resp.More {
			more = false
		} else {
			// Next page starts after the last key in the current page, add null byte to make it the next key
			lastKey := resp.Kvs[len(resp.Kvs)-1].Key
			startKey = string(append(lastKey, 0x00))
		}
	}

	return rev, nil
}

func (p *EtcdProvider) PutWithLease(ctx context.Context, key string, value string) error {
	if p.leaseID == 0 {
		return errors.New("leaseID is not set")
	}

	_, err := p.client.Put(ctx, key, value, clientv3.WithLease(p.leaseID))
	if err != nil {
		return err
	}
	return nil
}

func (p *EtcdProvider) Get(ctx context.Context, key string) (string, error) {
	resp, err := p.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	return string(resp.Kvs[0].Value), nil
}

func (p *EtcdProvider) Put(ctx context.Context, key string, value string) error {
	_, err := p.client.Put(ctx, key, value)
	if err != nil {
		return err
	}
	return nil
}

func (p *EtcdProvider) WatchByPrefix(ctx context.Context, prefix string) (chan Node, error) {
	ch := make(chan Node, 1000)
	go func() {
		defer close(ch)
		rev, err := p.snapshotByPrefix(ctx, prefix, ch)
		if err != nil {
			panic(err)
		}
		watcher := p.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))
		for resp := range watcher {
			if err := resp.Err(); err != nil {
				log.Printf("watch by prefix error: %v, skipping", err)
				continue
			}
			for _, event := range resp.Events {
				node := Node{Key: string(event.Kv.Key), Value: string(event.Kv.Value)}
				select {
				case ch <- node:
				case <-ctx.Done():
					ch <- Node{Key: "SMG_STOP", Value: ""}
					return
				}
			}
		}
	}()
	return ch, nil
}

func (p *EtcdProvider) Lease(ctx context.Context, key string, ttl int64) (bool, error) {
	grant, err := p.client.Grant(ctx, ttl)
	if err != nil {
		return false, err
	}

	txn, err := p.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "", clientv3.WithLease(grant.ID))).
		Commit()
	if err != nil {
		_, _ = p.client.Revoke(ctx, grant.ID)
		return false, err
	}
	if !txn.Succeeded {
		_, _ = p.client.Revoke(ctx, grant.ID)
		return false, nil
	}

	keepAlive, err := p.client.KeepAlive(ctx, grant.ID)
	if err != nil {
		_, _ = p.client.Revoke(ctx, grant.ID)
		return false, err
	}

	p.leaseID = grant.ID
	go func() {
		defer func() {
			revokeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = p.client.Revoke(revokeCtx, grant.ID)
		}()
		for {
			select {
			case _, ok := <-keepAlive:
				if !ok {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return true, nil
}

func (p *EtcdProvider) Resign(ctx context.Context) error {
	if p.leaseID == 0 {
		return nil
	}
	_, err := p.client.Revoke(ctx, p.leaseID)
	log.Printf("revoked lease: %v", err)
	p.leaseID = 0
	return err
}

func NewEtcdProvider(ctx context.Context, endpoint string) IEtcdProvider {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
	if err != nil {
		panic(err)
	}

	return &EtcdProvider{
		client: client,
	}
}
