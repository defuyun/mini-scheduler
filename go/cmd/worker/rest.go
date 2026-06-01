package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/defuyun/mini-scheduler/internal/utils"
	"github.com/defuyun/mini-scheduler/internal/worker"
)

type RestServer struct {
	agent worker.IWorkerAgent
}

func (s *RestServer) getWorkerInfo(w http.ResponseWriter, r *http.Request) {
	workerInfo := s.agent.GetWorkerInfo()
	json.NewEncoder(w).Encode(workerInfo)
}

func (s *RestServer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/worker", s.getWorkerInfo)
	return mux
}

func (s *RestServer) Start(ctx context.Context) error {
	srv := &http.Server{
		Addr:    utils.GetWorkerEndpoint(),
		Handler: s.routes(),
	}

	go func() {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	return srv.ListenAndServe()
}

func NewRestServer(agent worker.IWorkerAgent) *RestServer {
	return &RestServer{
		agent: agent,
	}
}
