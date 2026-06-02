package smg

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/defuyun/mini-scheduler/internal/shards"
	"github.com/defuyun/mini-scheduler/internal/worker"
)

func getWorkerIDFromKey(key string) string {
	parts := strings.Split(key, "/")

	if len(parts) < 1 {
		return ""
	}

	return parts[len(parts)-1]
}

func (m *ShardManager) onWorkerChanged(ctx context.Context, smgContext *ShardManagerContext, event Event) error {
	switch event.EventType {
	case WorkerJoined:
		var workerInfo worker.WorkerInfo
		err := json.Unmarshal([]byte(event.Data.(string)), &workerInfo)
		if err != nil {
			log.Printf("failed to unmarshal worker info: %v", err)
			return err
		}
		log.Printf("worker %s joined", workerInfo.WorkerID)
		smgContext.Workers[workerInfo.WorkerID] = workerInfo
	case WorkerLeft:
		workerID := getWorkerIDFromKey(event.Data.(string))
		var _, ok = smgContext.Workers[workerID]
		if ok {
			log.Printf("worker %s left", workerID)
			delete(smgContext.Workers, workerID)
		}
	}
	return nil
}

func (m *ShardManager) onShardPlanChanged(ctx context.Context, smgContext *ShardManagerContext, event Event) error {
	switch event.EventType {
	case ShardPlanUpdated:
		var shardPlan shards.ShardPlan
		err := json.Unmarshal([]byte(event.Data.(string)), &shardPlan)
		if err != nil {
			log.Printf("failed to unmarshal shard plan: %v", err)
			return err
		}
		smgContext.ShardPlan.Store(&shardPlan)
		return nil
	}
	return nil
}
