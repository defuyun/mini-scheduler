package smg

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync/atomic"

	etcdProvider "github.com/defuyun/mini-scheduler/internal/etcd"
	"github.com/defuyun/mini-scheduler/internal/shards"
	"github.com/defuyun/mini-scheduler/internal/utils"
)

type ShardManagerInfo struct {
	ServiceName  string `json:"service_name"`
	EtcdEndpoint string `json:"etcd_endpoint"`
}

type IShardManager interface {
	Join(ctx context.Context) error
	WatchEvents(ctx context.Context) error
	Shutdown(ctx context.Context) error
	GetShardManagerInfo() ShardManagerInfo
	PutShardPlan(ctx context.Context, shardPlan shards.ShardPlan) error
}

type ShardManager struct {
	shardManagerInfo ShardManagerInfo
	etcdProvider     etcdProvider.IEtcdProvider
	smgContext       atomic.Pointer[ShardManagerContext]
	routingPlan      atomic.Pointer[shards.RoutingPlan]
	eventQueues      map[EventType]*EventQueue
}

func (m *ShardManager) Join(ctx context.Context) error {
	shardManagerKey := utils.GetShardManagerKey(m.shardManagerInfo.ServiceName)
	lease, err := m.etcdProvider.Lease(ctx, shardManagerKey, 10)
	if err != nil {
		panic(err)
	}
	if !lease {
		panic("failed to lease")
	}
	return nil
}

func (m *ShardManager) Shutdown(ctx context.Context) error {
	return m.etcdProvider.Resign(ctx)
}

func (m *ShardManager) WatchEvents(ctx context.Context) error {
	serviceKey := utils.GetServiceKey(m.shardManagerInfo.ServiceName)
	workerKeyPrefix := utils.GetWorkerKey(m.shardManagerInfo.ServiceName, "")
	shardPlanKeyPrefix := utils.GetShardPlanKey(m.shardManagerInfo.ServiceName)
	routingPlanKeyPrefix := utils.GetRoutingPlanKey(m.shardManagerInfo.ServiceName)

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
				m.eventQueues[WorkerEvent].Enqueue(ctx, Event{EventType: WorkerLeft, Data: event.Key})
			} else {
				m.eventQueues[WorkerEvent].Enqueue(ctx, Event{EventType: WorkerJoined, Data: event.Value})
			}
		case strings.HasPrefix(event.Key, shardPlanKeyPrefix):
			if event.Value != "" {
				m.eventQueues[ShardPlanUpdated].Enqueue(ctx, Event{EventType: ShardPlanUpdated, Data: event.Value})
			}
		case strings.HasPrefix(event.Key, routingPlanKeyPrefix):
			if event.Value != "" {
				m.eventQueues[RoutingPlanUpdated].Enqueue(ctx, Event{EventType: RoutingPlanUpdated, Data: event.Value})
			}
		}
	}

	return nil
}

func (m *ShardManager) GetShardManagerInfo() ShardManagerInfo {
	return m.shardManagerInfo
}

func (m *ShardManager) PutShardPlan(ctx context.Context, shardPlan shards.ShardPlan) error {
	shardPlanJSON, err := json.Marshal(shardPlan)
	if err != nil {
		return err
	}
	return m.etcdProvider.Put(ctx, utils.GetShardPlanKey(m.shardManagerInfo.ServiceName), string(shardPlanJSON))
}

func NewShardManager(ctx context.Context, shardManagerInfo ShardManagerInfo, etcdProvider etcdProvider.IEtcdProvider) IShardManager {
	eventQueues := make(map[EventType]*EventQueue)
	smgContext := NewShardManagerContext()
	routingPlan := shards.NewRoutingPlan()

	shardManager := &ShardManager{
		shardManagerInfo: shardManagerInfo,
		etcdProvider:     etcdProvider,
		eventQueues:      eventQueues,
	}

	shardManager.smgContext.Store(smgContext)
	shardManager.routingPlan.Store(routingPlan)

	workerEventQueue := NewEventQueue(shardManager.onWorkerChanged, smgContext)
	workerEventQueue.Start(ctx)

	shardPlanEventQueue := NewEventQueue(shardManager.onShardPlanChanged, smgContext)
	shardPlanEventQueue.Start(ctx)

	routingPlanEventQueue := NewEventQueue(shardManager.onRoutingPlanChanged, smgContext)
	routingPlanEventQueue.Start(ctx)

	eventQueues[WorkerEvent] = workerEventQueue
	eventQueues[ShardPlanUpdated] = shardPlanEventQueue
	eventQueues[RoutingPlanUpdated] = routingPlanEventQueue

	return shardManager
}
