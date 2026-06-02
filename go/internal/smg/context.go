package smg

import (
	"sync/atomic"

	"github.com/defuyun/mini-scheduler/internal/shards"
	"github.com/defuyun/mini-scheduler/internal/worker"
)

type ShardManagerContext struct {
	Workers   map[string]worker.WorkerInfo
	ShardPlan atomic.Pointer[shards.ShardPlan]
}
