package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/defuyun/mini-scheduler/internal/etcd"
	"github.com/defuyun/mini-scheduler/internal/shards"
	"github.com/defuyun/mini-scheduler/internal/smg"
	"github.com/defuyun/mini-scheduler/internal/utils"
	"github.com/defuyun/mini-scheduler/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	etcdProvider := etcd.NewEtcdProvider(ctx, utils.GetEtcdEndpoint())
	shardManagerInfo := smg.ShardManagerInfo{
		ServiceName:  utils.GetServiceName(),
		EtcdEndpoint: utils.GetEtcdEndpoint(),
	}

	shardManagerContext := &smg.ShardManagerContext{
		Workers:   make(map[string]worker.WorkerInfo),
		ShardPlan: atomic.Pointer[shards.ShardPlan]{},
	}

	shardManager := smg.NewShardManager(ctx, shardManagerInfo, etcdProvider, shardManagerContext)
	err := shardManager.Join(ctx)
	if err != nil {
		log.Fatalf("failed to join shard manager: %v", err)
		panic(err)
	}

	go func() {
		err := shardManager.WatchEvents(ctx)
		if err != nil && err != context.Canceled {
			log.Printf("failed to watch events: %v", err)
		}
	}()

	restServer := NewRestServer(shardManager)
	err = restServer.Start(ctx)

	if err != nil && err != http.ErrServerClosed {
		log.Printf("failed to start rest server: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	shardManager.Shutdown(shutdownCtx)
}
