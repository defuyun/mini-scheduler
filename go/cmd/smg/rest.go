package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/defuyun/mini-scheduler/internal/smg"
	"github.com/defuyun/mini-scheduler/internal/utils"
)

type RestServer struct {
	shardManager smg.IShardManager
}

func (s *RestServer) getShardManagerInfo(w http.ResponseWriter, r *http.Request) {
	shardManagerInfo := s.shardManager.GetShardManagerInfo()
	json.NewEncoder(w).Encode(shardManagerInfo)
}

func (s *RestServer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/shard-manager", s.getShardManagerInfo)
	return mux
}

func (s *RestServer) Start(ctx context.Context) error {
	srv := &http.Server{
		Addr:    utils.GetShardManagerEndpoint(),
		Handler: s.routes(),
	}

	go func() {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	return srv.ListenAndServe()
}

func NewRestServer(shardManager smg.IShardManager) *RestServer {
	return &RestServer{
		shardManager: shardManager,
	}
}
