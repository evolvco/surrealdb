// Package wire defines the CBOR wire format exchanged between the CDC
// connector (Go, sidecar) and SurrealDB's /cdc/ingest endpoint (Rust, main
// process).
//
// Framing on the wire: length-prefixed messages in a streaming POST body.
//
//	[4 bytes: big-endian uint32 payload length]
//	[N bytes: CBOR-encoded Message]
//	[repeat]
//
// Messages are a tagged union (CBOR map with "type" discriminator). The same
// shape is decoded on the Rust side via serde with #[serde(tag = "type")].
package wire

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
)

// OpType mirrors common.OpType from TiCDC. Kept as a separate enum so we do
// not leak TiCDC types into the wire format: the Rust side has no dependency
// on TiCDC.
type OpType uint8

const (
	OpTypeUnknown OpType = 0
	OpTypePut     OpType = 1
	OpTypeDelete  OpType = 2
)

// Message is the tagged-union envelope written to the wire.
//
// Exactly one of the optional fields is populated per frame, determined by
// the Type field. CBOR omits omitempty fields, so on-the-wire each frame
// carries only the relevant payload.
type Message struct {
	RowChange  *RowChange  `cbor:"row_change,omitempty"`
	ResolvedTs *ResolvedTs `cbor:"resolved_ts,omitempty"`
	Heartbeat  *Heartbeat  `cbor:"heartbeat,omitempty"`
}

// RowChange mirrors the fields of common.RawKVEntry that SurrealDB needs in
// order to dispatch LIVE SELECT notifications. CRTs (commit ts) and StartTs
// are preserved for future ordering/dedup work even though the initial
// handler does not use them.
type RowChange struct {
	SubscriptionID uint64 `cbor:"subscription_id"`
	Op             OpType `cbor:"op"`
	Key            []byte `cbor:"key"`
	Value          []byte `cbor:"value,omitempty"`     // nil for deletes
	OldValue       []byte `cbor:"old_value,omitempty"` // nil for inserts
	CommitTs       uint64 `cbor:"commit_ts"`
	StartTs        uint64 `cbor:"start_ts"`
	RegionID       uint64 `cbor:"region_id"`
}

// ResolvedTs advances the consumer watermark. The connector emits one of
// these per advanceResolvedTs callback from the logpuller.
type ResolvedTs struct {
	SubscriptionID uint64 `cbor:"subscription_id"`
	Ts             uint64 `cbor:"ts"`
}

// Heartbeat is a liveness ping. Emitted on a timer regardless of whether
// the KV stream is producing events, so SurrealDB can detect a stalled
// connector distinct from a quiet cluster.
type Heartbeat struct {
	EmittedAt int64 `cbor:"emitted_at"` // unix micros
}

// WriteFrame serialises msg as CBOR and writes it as a length-prefixed
// frame to w. The length prefix is 4 bytes big-endian.
//
// Returns an error if either the encode or the write fails. Partial writes
// are not handled here: the expected caller is a net/http request body
// writer which flushes at its own cadence; if the underlying stream is
// broken the error propagates up to trigger a reconnect.
func WriteFrame(w io.Writer, msg *Message) error {
	payload, err := cbor.Marshal(msg)
	if err != nil {
		return fmt.Errorf("cbor marshal: %w", err)
	}
	if len(payload) > (1 << 31) {
		return fmt.Errorf("frame too large: %d bytes", len(payload))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write length prefix: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}
