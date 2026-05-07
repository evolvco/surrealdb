// Package config loads the connector's runtime configuration from
// environment variables.
//
// All configuration is env-driven so the sidecar container is stateless
// and can be restarted, moved between pods, or relocated to a separate
// deployment without any file-based config changes.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full runtime configuration for the connector.
type Config struct {
	// PDEndpoints is the comma-separated list of PD (Placement Driver)
	// addresses the connector uses to subscribe to TiKV, e.g.
	// "pd-0:2379,pd-1:2379,pd-2:2379".
	PDEndpoints []string

	// SurrealIngestURL is the full URL of SurrealDB's /cdc/ingest
	// endpoint, e.g. "http://localhost:8000/cdc/ingest".
	SurrealIngestURL string

	// SurrealUsername / SurrealPassword are the credentials presented to
	// SurrealDB as HTTP Basic auth on every ingest request. Must be a
	// root-level account (the /cdc/ingest handler rejects non-root
	// sessions).
	SurrealUsername string
	SurrealPassword string

	// StartKey / EndKey bound the key range the connector subscribes to
	// in TiKV. By default the connector subscribes to the entire key
	// space (empty start / empty end), which is correct when SurrealDB
	// is the sole tenant of the TiKV cluster.
	StartKey []byte
	EndKey   []byte

	// HeartbeatInterval is how often the connector emits a Heartbeat
	// frame to SurrealDB, independent of KV traffic.
	HeartbeatInterval time.Duration

	// ReconnectInitialBackoff is the starting backoff when the HTTP
	// stream to SurrealDB drops. Doubles up to ReconnectMaxBackoff.
	ReconnectInitialBackoff time.Duration
	ReconnectMaxBackoff     time.Duration

	// RegionRequestWorkers is passed through to the TiCDC
	// SubscriptionClientConfig. One worker per TiKV store is typical.
	RegionRequestWorkers uint
}

// Load reads configuration from the environment and returns a validated
// Config. Required variables cause an error if unset; optional ones fall
// back to sensible defaults documented per-field.
func Load() (*Config, error) {
	cfg := &Config{
		HeartbeatInterval:       5 * time.Second,
		ReconnectInitialBackoff: 1 * time.Second,
		ReconnectMaxBackoff:     60 * time.Second,
		RegionRequestWorkers:    1,
	}

	pd := os.Getenv("CDC_PD_ENDPOINTS")
	if pd == "" {
		return nil, fmt.Errorf("CDC_PD_ENDPOINTS is required (comma-separated list)")
	}
	cfg.PDEndpoints = splitAndTrim(pd)

	cfg.SurrealIngestURL = os.Getenv("CDC_SURREAL_INGEST_URL")
	if cfg.SurrealIngestURL == "" {
		return nil, fmt.Errorf("CDC_SURREAL_INGEST_URL is required")
	}

	cfg.SurrealUsername = os.Getenv("CDC_SURREAL_USERNAME")
	if cfg.SurrealUsername == "" {
		return nil, fmt.Errorf("CDC_SURREAL_USERNAME is required")
	}
	cfg.SurrealPassword = os.Getenv("CDC_SURREAL_PASSWORD")
	if cfg.SurrealPassword == "" {
		return nil, fmt.Errorf("CDC_SURREAL_PASSWORD is required")
	}

	// Key range defaults: empty StartKey = beginning of key space,
	// EndKey = 0xFF covers all SurrealDB data keys (which start with
	// 0x2F = '/') while excluding TiKV internal sentinel regions above
	// 0xFF. These raw bytes get memcomparable-encoded in
	// installSubscription() before being passed to the logpuller.
	if v := os.Getenv("CDC_START_KEY"); v != "" {
		cfg.StartKey = []byte(v)
	} else {
		cfg.StartKey = []byte{}
	}
	if v := os.Getenv("CDC_END_KEY"); v != "" {
		cfg.EndKey = []byte(v)
	} else {
		cfg.EndKey = []byte{0xFF}
	}

	if v := os.Getenv("CDC_HEARTBEAT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("CDC_HEARTBEAT_INTERVAL: %w", err)
		}
		cfg.HeartbeatInterval = d
	}
	if v := os.Getenv("CDC_RECONNECT_INITIAL_BACKOFF"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("CDC_RECONNECT_INITIAL_BACKOFF: %w", err)
		}
		cfg.ReconnectInitialBackoff = d
	}
	if v := os.Getenv("CDC_RECONNECT_MAX_BACKOFF"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("CDC_RECONNECT_MAX_BACKOFF: %w", err)
		}
		cfg.ReconnectMaxBackoff = d
	}
	if v := os.Getenv("CDC_REGION_REQUEST_WORKERS"); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("CDC_REGION_REQUEST_WORKERS: %w", err)
		}
		cfg.RegionRequestWorkers = uint(n)
	}

	return cfg, nil
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
