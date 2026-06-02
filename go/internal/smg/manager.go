package smg

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/defuyun/mini-scheduler/internal/constants"
	etcdProvider "github.com/defuyun/mini-scheduler/internal/etcd"
	"github.com/defuyun/mini-scheduler/internal/worker"
)

type ShardManagerInfo struct {
	ServiceName  string `json:"service_name"`
	EtcdEndpoint string `json:"etcd_endpoint"`
}

type IShardManager interface {
	Join(ctx context.Context) error
	WatchEvents(ctx context.Context) error
	GetShardManagerInfo() ShardManagerInfo
}

type ShardManager struct {
	shardManagerInfo ShardManagerInfo
	etcdProvider     etcdProvider.IEtcdProvider
}

func (m *ShardManager) Join(ctx context.Context) error {
	shardManagerKey := constants.GetShardManagerKey(m.shardManagerInfo.ServiceName)
	lease, err := m.etcdProvider.Lease(ctx, shardManagerKey, 10)
	if err != nil {
		panic(err)
	}
	if !lease {
		panic("failed to lease")
	}
	return nil
}

func (m *ShardManager) WatchEvents(ctx context.Context) error {
	serviceKey := constants.GetServiceKey(m.shardManagerInfo.ServiceName)
	workerKeyPrefix := constants.GetWorkerKey(m.shardManagerInfo.ServiceName, "")

	ch, err := m.etcdProvider.WatchByPrefix(ctx, serviceKey)
	if err != nil {
		log.Printf("failed to watch workers: %v", err)
		panic(err)
	}

	for event := range ch {
		if event.Key == "SMG_STOP" {
			log.Println("shard manager watch stopped")
			return nil
		}

		switch {
		case strings.HasPrefix(event.Key, workerKeyPrefix):
			if event.Value == "" {
				log.Printf("worker %s left", event.Key)
				continue
			}

			var workerInfo worker.WorkerInfo
			err := json.Unmarshal([]byte(event.Value), &workerInfo)
			if err != nil {
				log.Printf("failed to unmarshal worker info: %v", err)
			}
			log.Printf("worker %s joined", workerInfo.WorkerID)
		}
	}

	return nil
}

func (m *ShardManager) GetShardManagerInfo() ShardManagerInfo {
	return m.shardManagerInfo
}

func NewShardManager(shardManagerInfo ShardManagerInfo, etcdProvider etcdProvider.IEtcdProvider) IShardManager {
	return &ShardManager{
		shardManagerInfo: shardManagerInfo,
		etcdProvider:     etcdProvider,
	}
}
