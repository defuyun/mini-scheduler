package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync/atomic"

	etcdProvider "github.com/defuyun/mini-scheduler/internal/etcd"
	shards "github.com/defuyun/mini-scheduler/internal/shards"
	"github.com/defuyun/mini-scheduler/internal/utils"
)

/*
When a worker starts up it will create a key in etcd with the worker's ID. worker id will be the pod name
*/

const WORKER_LEASE_TTL_S = 10

type IWorkerAgent interface {
	Join(ctx context.Context) error
	Shutdown(ctx context.Context) error
	GetWorkerInfo() WorkerInfo
}

type WorkerInfo struct {
	ServiceName string `json:"service_name"`
	WorkerID    string `json:"worker_id"`
	Endpoint    string `json:"endpoint"`
}

type WorkerAgent struct {
	workerInfo       WorkerInfo
	etcdProvider     etcdProvider.IEtcdProvider
	shardAssignments atomic.Pointer[[]shards.Shard]
}

func (w *WorkerAgent) Join(ctx context.Context) error {
	workerKey := utils.GetWorkerKey(w.workerInfo.ServiceName, w.workerInfo.WorkerID)
	lease, err := w.etcdProvider.Lease(ctx, workerKey, 10)
	if err != nil {
		log.Printf("failed to lease worker key: %v", err)
		return err
	}
	if !lease {
		log.Printf("failed to lease worker key")
		return errors.New("failed to lease worker key")
	}

	workerInfoJSON, err := json.Marshal(w.workerInfo)
	if err != nil {
		log.Printf("failed to marshal worker info: %v", err)
		return err
	}

	err = w.etcdProvider.PutWithLease(ctx, workerKey, string(workerInfoJSON))
	if err != nil {
		log.Printf("failed to put worker key: %v", err)
		return err
	}

	return nil
}

func (w *WorkerAgent) WatchShards(ctx context.Context) error {
	workerShardsKey := utils.GetWorkerShardsKey(w.workerInfo.ServiceName, w.workerInfo.WorkerID)
	ch, err := w.etcdProvider.WatchByPrefix(ctx, workerShardsKey)
	if err != nil {
		log.Printf("failed to watch shards: %v", err)
		return err
	}

	for shard := range ch {
		if shard.Key == "SMG_STOP" {
			log.Println("shards watch stopped")
			return nil
		}

		var shardAssignments []shards.Shard

		if shard.Value == "" {
			shardAssignments = []shards.Shard{}
			w.shardAssignments.Store(&shardAssignments)
			continue
		}

		err := json.Unmarshal([]byte(shard.Value), &shardAssignments)
		if err != nil {
			log.Printf("failed to unmarshal shard assignments: %v", err)
			continue
		}

		w.shardAssignments.Store(&shardAssignments)
	}

	return nil
}

func (w *WorkerAgent) Shutdown(ctx context.Context) error {
	return w.etcdProvider.Resign(ctx)
}

func (w *WorkerAgent) GetWorkerInfo() WorkerInfo {
	return w.workerInfo
}

func NewWorkerAgent(workerInfo WorkerInfo, etcdProvider etcdProvider.IEtcdProvider) IWorkerAgent {
	return &WorkerAgent{
		workerInfo:   workerInfo,
		etcdProvider: etcdProvider,
	}
}
