package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/defuyun/mini-scheduler/internal/etcd"
	"github.com/defuyun/mini-scheduler/internal/utils"
	"github.com/defuyun/mini-scheduler/internal/worker"
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

	serviceName := utils.GetServiceName()
	workerID := utils.NewULID()

	workerInfo := worker.WorkerInfo{
		ServiceName: serviceName,
		WorkerID:    workerID,
		Endpoint:    utils.GetWorkerEndpoint(),
	}
	workerAgent := worker.NewWorkerAgent(workerInfo, etcdProvider)
	workerAgent.Join(ctx)

	restServer := NewRestServer(workerAgent)
	err = restServer.Start(ctx)
	if err != nil && err != http.ErrServerClosed {
		log.Printf("failed to start rest server: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	workerAgent.Shutdown(shutdownCtx)
}
