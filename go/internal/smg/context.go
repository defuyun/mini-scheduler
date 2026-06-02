package smg

import (
	"github.com/defuyun/mini-scheduler/internal/shards"
	"github.com/defuyun/mini-scheduler/internal/worker"
)

type ShardManagerContext struct {
	Workers map[string]worker.WorkerInfo
	Shards  map[string]shards.Shard
}
