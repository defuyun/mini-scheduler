package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/defuyun/mini-scheduler/internal/etcd"
	"github.com/defuyun/mini-scheduler/internal/smg"
	"github.com/defuyun/mini-scheduler/internal/utils"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	etcdProvider := etcd.NewEtcdProvider(ctx, utils.GetEtcdEndpoint())
	lease, err := etcdProvider.Lease(ctx, utils.GetServiceName(), 10)
	if err != nil {
		log.Fatalf("failed to lease: %v", err)
		panic(err)
	}
	if !lease {
		log.Fatalf("failed to lease")
		panic("failed to lease")
	}

	shardManagerInfo := smg.ShardManagerInfo{
		ServiceName:  utils.GetServiceName(),
		EtcdEndpoint: utils.GetEtcdEndpoint(),
	}
	shardManager := smg.NewShardManager(shardManagerInfo, etcdProvider)
	shardManager.Join(ctx)

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
}
