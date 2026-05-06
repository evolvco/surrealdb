// Command cdc-connector is a sidecar that subscribes to TiKV CDC via
// TiCDC's logpuller and forwards events to SurrealDB over HTTP.
//
// See cdc-connector/README.md for deployment details.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/surrealdb/surrealdb/cdc-connector/internal/config"
	"github.com/surrealdb/surrealdb/cdc-connector/internal/connector"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("config load failed", zap.Error(err))
	}

	// Signal handling: SIGTERM from Kubernetes triggers a graceful
	// shutdown via context cancellation. SIGINT for local dev.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c := connector.New(cfg)
	if err := c.Run(ctx); err != nil {
		log.Error("connector exited with error", zap.Error(err))
		os.Exit(1)
	}
	log.Info("connector exited cleanly")
}
