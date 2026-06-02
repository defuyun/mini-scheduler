package smg

import "context"

type EventType string
type EventCallback func(ctx context.Context, smgContext *ShardManagerContext, event Event) error

const (
	WorkerEvent        EventType = "worker"
	WorkerJoined       EventType = "worker_joined"
	WorkerLeft         EventType = "worker_left"
	ShardPlanUpdated   EventType = "shard_plan_updated"
	RoutingPlanUpdated EventType = "routing_plan_updated"
)

type Event struct {
	EventType EventType
	Data      interface{}
}
