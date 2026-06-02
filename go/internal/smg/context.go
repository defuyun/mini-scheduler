package smg

import (
	"github.com/defuyun/mini-scheduler/internal/shards"
	"github.com/defuyun/mini-scheduler/internal/worker"
)

type ShardManagerContext struct {
	Workers   map[string]worker.WorkerInfo
	ShardPlan shards.ShardPlan
}

func NewShardManagerContext() *ShardManagerContext {
	return &ShardManagerContext{
		Workers:   make(map[string]worker.WorkerInfo),
		ShardPlan: shards.ShardPlan{},
	}
}
