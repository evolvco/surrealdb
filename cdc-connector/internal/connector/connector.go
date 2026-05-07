// Package connector wires the TiCDC logpuller to SurrealDB's /cdc/ingest
// endpoint.
//
// The flow is:
//
//	logpuller → consumeKVEvents callback → event channel → HTTP POST body
//
// The channel exists to decouple the synchronous logpuller callbacks from
// the asynchronous HTTP forwarder. If the forwarder can't keep up the
// channel fills, Enqueue returns false, and the logpuller applies
// backpressure via the wakeCallback mechanism.
package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	ticdcconfig "github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/util/codec"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"

	"github.com/surrealdb/surrealdb/cdc-connector/internal/config"
	"github.com/surrealdb/surrealdb/cdc-connector/internal/wire"
)

// syntheticTableID is used as span.TableID when subscribing. The logpuller
// requires a non-zero TableID (it panics otherwise) but uses the value only
// as an identifier for logging/routing; it has no semantic meaning for
// SurrealDB which is not table-partitioned at the TiKV layer.
const syntheticTableID = 1

// eventChannelCapacity bounds in-flight frames between the logpuller
// callback and the HTTP forwarder. When full, consumeKVEvents returns false
// which the logpuller respects as backpressure.
const eventChannelCapacity = 4096

// Connector is the top-level unit: it owns the logpuller, the event
// channel, and the HTTP forwarder loop.
type Connector struct {
	cfg *config.Config

	subClient    logpuller.SubscriptionClient
	pdClient     pd.Client
	subID        logpuller.SubscriptionID
	events     chan *wire.Message
	httpClient *http.Client
}

// New constructs a Connector but does not start any goroutines. Call Run
// to start the subscription and HTTP forwarder.
func New(cfg *config.Config) *Connector {
	return &Connector{
		cfg:    cfg,
		events: make(chan *wire.Message, eventChannelCapacity),
		httpClient: &http.Client{
			// No overall request timeout: the POST body is intentionally
			// long-lived (hours or days). Per-write deadlines are handled
			// implicitly by TCP keepalives and the context passed to Run.
			Timeout: 0,
		},
	}
}

// Run bootstraps the TiCDC subscription client, starts its background
// goroutines, installs the subscription, and runs the HTTP forwarder loop
// until ctx is cancelled.
//
// Returns the first fatal error from any of the background goroutines, or
// nil on clean shutdown.
func (c *Connector) Run(ctx context.Context) error {
	log.Info("cdc-connector starting",
		zap.Strings("pd_endpoints", c.cfg.PDEndpoints),
		zap.String("ingest_url", c.cfg.SurrealIngestURL))

	if err := c.bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	// The subscription client has its own Run() goroutine; launch it.
	subErrCh := make(chan error, 1)
	go func() {
		subErrCh <- c.subClient.Run(ctx)
	}()

	// Install the subscription once the client is running.
	c.installSubscription()

	// Run the forwarder on this goroutine; it blocks until ctx or fatal
	// error, then we unwind in reverse.
	fwdErr := c.forwarderLoop(ctx)

	log.Info("cdc-connector shutting down")
	c.subClient.Unsubscribe(c.subID)
	if err := c.subClient.Close(ctx); err != nil {
		log.Warn("subscription client close", zap.Error(err))
	}
	if c.pdClient != nil {
		c.pdClient.Close()
	}

	// Report whichever error surfaced first.
	select {
	case err := <-subErrCh:
		if err != nil && err != context.Canceled {
			return fmt.Errorf("subscription client: %w", err)
		}
	default:
	}
	return fwdErr
}

// bootstrap performs the one-time setup: global config, PD client, region
// cache, pdutil.Clock, keyspace manager, lock resolver, SubscriptionClient.
// These must be registered in the appcontext singleton before
// NewSubscriptionClient (or NewLockerResolver) is called because those
// consumers read them back via appcontext.GetService[T]().
func (c *Connector) bootstrap(ctx context.Context) error {
	// TiKV's CDC module requires a valid semver TiCDC version in the gRPC
	// request header. Without it, TiKV suppresses resolved-ts events for
	// the downstream. We impersonate a compatible TiCDC version.
	version.ReleaseVersion = "v8.5.7"

	// TiCDC reads a global server config via config.GetGlobalServerConfig()
	ticdcconfig.StoreGlobalServerConfig(ticdcconfig.GetDefaultServerConfig())

	// PD client — the entry point to the TiKV cluster topology.
	pdClient, err := pd.NewClientWithContext(
		ctx,
		c.cfg.PDEndpoints,
		pd.SecurityOption{},
	)
	if err != nil {
		return fmt.Errorf("new pd client: %w", err)
	}
	c.pdClient = pdClient

	// Region cache — maps keys → TiKV store addresses. Built on top of
	// the PD client. The logpuller looks this up via appcontext.
	regionCache := tikv.NewRegionCache(pdClient)
	appcontext.SetService(appcontext.RegionCache, regionCache)

	// PD clock — monotonic time source reconciled with PD. Also looked
	// up via appcontext. Must call Run() to keep the clock updated;
	// without it the logpuller's event loop stalls.
	pdClock, err := pdutil.NewClock(ctx, pdClient)
	if err != nil {
		return fmt.Errorf("new pd clock: %w", err)
	}
	pdClock.Run(ctx)
	appcontext.SetService(appcontext.DefaultPDClock, pdClock)

	// Keyspace manager — required by the lock resolver to find which TiKV
	// storage handle to use when scanning locks. In classic (single-
	// keyspace) mode this resolves to keyspace 0. Requires http:// prefix.
	pdEndpointsWithScheme := make([]string, len(c.cfg.PDEndpoints))
	for i, ep := range c.cfg.PDEndpoints {
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			pdEndpointsWithScheme[i] = "http://" + ep
		} else {
			pdEndpointsWithScheme[i] = ep
		}
	}
	ksManager := keyspace.NewManager(pdEndpointsWithScheme)
	appcontext.SetService(appcontext.KeyspaceManager, ksManager)

	// Lock resolver — cleans up stale txn locks. Takes no arguments;
	// reads the keyspace manager from appcontext at resolve time.
	lockResolver := txnutil.NewLockerResolver()

	// SubscriptionClient proper. Security is unset: we rely on TiKV
	// being in-cluster and unauthenticated. If mTLS is added later this
	// is where it plugs in.
	credential := &security.Credential{}
	subCfg := &logpuller.SubscriptionClientConfig{
		RegionRequestWorkerPerStore: c.cfg.RegionRequestWorkers,
	}
	c.subClient = logpuller.NewSubscriptionClient(subCfg, pdClient, lockResolver, credential)

	return nil
}

// installSubscription allocates an ID and subscribes to the configured
// key range. The two callbacks are the plug points that feed the
// forwarder loop.
func (c *Connector) installSubscription() {
	c.subID = c.subClient.AllocSubscriptionID()

	// Keys must be memcomparable-encoded to match the format used by PD
	// and TiKV's CDC module. Raw bytes won't work — TiKV rejects them
	// and refuses to advance resolved-ts for the downstream.
	startKey := codec.EncodeBytes(nil, c.cfg.StartKey)
	endKey := codec.EncodeBytes(nil, c.cfg.EndKey)

	span := heartbeatpb.TableSpan{
		TableID:  syntheticTableID,
		StartKey: startKey,
		EndKey:   endKey,
	}

	// Get the current cluster timestamp from PD. Subscribing at ts=0
	// would mean "from the beginning of time" which is before GC and
	// causes the range-lock to never converge. We subscribe from "now"
	// so we only see changes going forward.
	physical, logical, err := c.pdClient.GetTS(context.Background())
	if err != nil {
		log.Warn("failed to get start ts from PD, using 0", zap.Error(err))
		physical, logical = 0, 0
	}
	var startTs uint64
	if physical > 0 {
		startTs = uint64(physical)<<18 | uint64(logical)
	}

	log.Info("installing subscription",
		zap.Uint64("subscription_id", uint64(c.subID)),
		zap.Uint64("start_ts", startTs),
		zap.Binary("start_key", c.cfg.StartKey),
		zap.Binary("end_key", c.cfg.EndKey))

	var advanceIntervalMs int64 = 1000
	const bdrMode = false

	c.subClient.Subscribe(
		c.subID,
		span,
		startTs,
		c.consumeKVEvents,
		c.advanceResolvedTs,
		advanceIntervalMs,
		bdrMode,
	)
}

// consumeKVEvents is invoked by the logpuller on its own goroutine with a
// batch of decoded KV entries.
//
// Return value convention (from the logpuller contract):
//   - false: batch consumed synchronously, keep sending.
//   - true: batch accepted asynchronously; logpuller pauses until
//     wakeCallback is invoked.
//
// We use a blocking channel send to apply natural backpressure: if
// SurrealDB can't keep up, the channel fills, this callback blocks,
// which blocks the logpuller's event handler, which propagates
// backpressure all the way to TiKV. No events are dropped.
func (c *Connector) consumeKVEvents(entries []common.RawKVEntry, wakeCallback func()) bool {
	for i := range entries {
		e := &entries[i]
		msg := &wire.Message{
			RowChange: &wire.RowChange{
				SubscriptionID: uint64(c.subID),
				Op:             mapOpType(e.OpType),
				Key:            e.Key,
				Value:          e.Value,
				OldValue:       e.OldValue,
				CommitTs:       e.CRTs,
				StartTs:        e.StartTs,
				RegionID:       e.RegionID,
			},
		}
		c.events <- msg
	}
	_ = wakeCallback
	return false
}

// advanceResolvedTs is invoked when the logpuller's watermark advances.
func (c *Connector) advanceResolvedTs(ts uint64) {
	msg := &wire.Message{
		ResolvedTs: &wire.ResolvedTs{
			SubscriptionID: uint64(c.subID),
			Ts:             ts,
		},
	}
	c.events <- msg
}

func mapOpType(op common.OpType) wire.OpType {
	switch op {
	case common.OpTypePut:
		return wire.OpTypePut
	case common.OpTypeDelete:
		return wire.OpTypeDelete
	default:
		return wire.OpTypeUnknown
	}
}

// forwarderLoop maintains a single long-lived HTTP POST to SurrealDB,
// streaming CBOR frames from the events channel into the request body.
// On disconnect it reconnects with exponential backoff.
func (c *Connector) forwarderLoop(ctx context.Context) error {
	backoff := c.cfg.ReconnectInitialBackoff
	heartbeatTicker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		if ctx.Err() != nil {
			return nil
		}

		err := c.runOnePost(ctx, heartbeatTicker)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log.Warn("ingest stream ended; reconnecting",
				zap.Duration("backoff", backoff),
				zap.Error(err))
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > c.cfg.ReconnectMaxBackoff {
			backoff = c.cfg.ReconnectMaxBackoff
		}
	}
}

// runOnePost opens one HTTP POST to /cdc/ingest and pumps frames from the
// event channel into its request body until an error occurs or ctx is
// cancelled. The returned error is non-nil on stream failure only.
func (c *Connector) runOnePost(ctx context.Context, heartbeats *time.Ticker) error {
	pr, pw := io.Pipe()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.SurrealIngestURL, pr)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/cbor")
	req.SetBasicAuth(c.cfg.SurrealUsername, c.cfg.SurrealPassword)
	// Tells net/http not to buffer the body; we want true streaming.
	req.TransferEncoding = []string{"chunked"}

	// Launch the request in a goroutine; its completion signals that the
	// server closed the stream or the connection broke.
	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := c.httpClient.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	// Pump frames until the request context is cancelled, the HTTP call
	// returns (success or error), or a write fails.
	writeErr := c.pumpFrames(ctx, pw, heartbeats)
	// Closing the write side flushes and allows the client goroutine to
	// finish the request.
	_ = pw.Close()

	// Wait for the HTTP request to complete. If we got a response, check
	// its status; non-2xx is a stream failure.
	select {
	case resp := <-respCh:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if writeErr != nil {
				return writeErr
			}
			return nil
		}
		return fmt.Errorf("ingest returned %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	case err := <-errCh:
		if writeErr != nil {
			return writeErr
		}
		return err
	}
}

// pumpFrames is the inner write loop. It sends frames from the events
// channel and emits Heartbeat frames on the heartbeat tick until an
// error or ctx cancellation.
func (c *Connector) pumpFrames(ctx context.Context, pw io.Writer, heartbeats *time.Ticker) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-c.events:
			if err := wire.WriteFrame(pw, msg); err != nil {
				return fmt.Errorf("write frame: %w", err)
			}
		case <-heartbeats.C:
			hb := &wire.Message{
				Heartbeat: &wire.Heartbeat{
					EmittedAt: time.Now().UnixMicro(),
				},
			}
			if err := wire.WriteFrame(pw, hb); err != nil {
				return fmt.Errorf("write heartbeat: %w", err)
			}
		}
	}
}
