//! HTTP ingest endpoint for Change Data Capture events.
//!
//! Consumes the length-prefixed CBOR stream produced by the external Go
//! `cdc-connector` sidecar and dispatches each row change into the
//! datastore's live-query notification pipeline via
//! [`Datastore::process_cdc_row`].
//!
//! The wire format is documented alongside the connector in
//! `cdc-connector/internal/wire/wire.go`. Summary:
//!
//! * Framing: `[u32 big-endian payload length][N bytes CBOR message]`, repeated.
//! * Message envelope is a tagged union with `type` discriminator
//!   (`row_change`, `resolved_ts`, `heartbeat`).
//!
//! Auth: only root-level sessions may POST here. The route capability must
//! also be enabled (it is by default when HTTP routes are allowed).

use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Extension, Router};
use futures::StreamExt;
use serde::Deserialize;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::dbs::Session;
use surrealdb::kvs::{CdcRowOp, Datastore};

use crate::err::Error;
use crate::net::AppState;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/cdc/ingest", post(handler))
		// The body is an open-ended stream; disable the default body limit
		// so we never truncate a long-lived connector session.
		.route_layer(DefaultBodyLimit::disable())
}

/// Wire envelope. One of `row_change`, `resolved_ts`, `heartbeat` is
/// populated based on `type`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(dead_code)]
enum CdcMessage {
	RowChange(RowChange),
	ResolvedTs(ResolvedTs),
	Heartbeat(Heartbeat),
}

#[derive(Debug, Deserialize)]
struct RowChange {
	#[allow(dead_code)]
	subscription_id: u64,
	op: WireOpType,
	key: Vec<u8>,
	#[serde(default)]
	value: Option<Vec<u8>>,
	#[serde(default)]
	old_value: Option<Vec<u8>>,
	#[allow(dead_code)]
	commit_ts: u64,
	#[allow(dead_code)]
	start_ts: u64,
	#[allow(dead_code)]
	region_id: u64,
}

#[derive(Debug, Deserialize)]
struct ResolvedTs {
	#[allow(dead_code)]
	subscription_id: u64,
	#[allow(dead_code)]
	ts: u64,
}

#[derive(Debug, Deserialize)]
struct Heartbeat {
	#[allow(dead_code)]
	emitted_at: i64,
}

/// Mirrors the connector's `OpType` enum. Kept as its own type so the
/// handler doesn't depend on TiCDC vocabulary directly.
#[derive(Debug, Deserialize)]
#[serde(from = "u8")]
enum WireOpType {
	Unknown,
	Put,
	Delete,
}

impl From<u8> for WireOpType {
	fn from(v: u8) -> Self {
		match v {
			1 => WireOpType::Put,
			2 => WireOpType::Delete,
			_ => WireOpType::Unknown,
		}
	}
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	request: Request,
) -> Result<impl IntoResponse, Error> {
	let db = &state.datastore;

	// Route capability gate.
	if !db.allows_http_route(&RouteTarget::CdcIngest) {
		warn!(
			"Capabilities denied HTTP route request attempt, target: '{}'",
			&RouteTarget::CdcIngest
		);
		return Err(Error::ForbiddenRoute(RouteTarget::CdcIngest.to_string()));
	}

	// CDC ingest is a system-level data path. Only root sessions may push
	// events; any lesser auth level would be able to synthesise fake live
	// notifications for records they already have edit rights on.
	if !session.au.is_root() {
		warn!("Rejected /cdc/ingest from non-root session");
		return Err(Error::InvalidAuth);
	}

	// Read the body as a byte stream and parse length-prefixed CBOR frames
	// incrementally. A single buffer accumulates partial reads across
	// multiple stream chunks; once enough bytes are available to satisfy
	// the next frame we decode and dispatch it.
	let mut body_stream = request.into_body().into_data_stream();
	let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);

	while let Some(chunk) = body_stream.next().await {
		let chunk = chunk.map_err(|e| {
			error!("cdc ingest stream error: {e}");
			Error::Request
		})?;
		buf.extend_from_slice(&chunk);
		process_buffered_frames(db, &mut buf).await?;
	}

	// Drain any final completed frame that arrived with the stream close.
	process_buffered_frames(db, &mut buf).await?;

	Ok(axum::http::StatusCode::NO_CONTENT)
}

/// Pulls complete `[len][payload]` frames out of `buf`, decodes them, and
/// dispatches each into the datastore. Any trailing partial frame remains
/// in `buf` for the next iteration of the read loop.
async fn process_buffered_frames(db: &Datastore, buf: &mut Vec<u8>) -> Result<(), Error> {
	loop {
		if buf.len() < 4 {
			return Ok(());
		}
		let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
		if buf.len() < 4 + len {
			return Ok(());
		}
		// Decode the payload, then drop the consumed bytes from the front
		// of the buffer.
		let payload = &buf[4..4 + len];
		let msg: CdcMessage = ciborium::de::from_reader(payload).map_err(|e| {
			warn!("cdc ingest cbor decode failed: {e}");
			Error::Request
		})?;
		let consumed = 4 + len;
		buf.drain(..consumed);

		dispatch(db, msg).await?;
	}
}

async fn dispatch(db: &Datastore, msg: CdcMessage) -> Result<(), Error> {
	match msg {
		CdcMessage::RowChange(row) => {
			let op = match row.op {
				WireOpType::Put => CdcRowOp::Put,
				WireOpType::Delete => CdcRowOp::Delete,
				WireOpType::Unknown => {
					trace!("cdc ingest skipping unknown op");
					return Ok(());
				}
			};
			db.process_cdc_row(op, &row.key, row.value.as_deref(), row.old_value.as_deref())
				.await
				.map_err(Error::from)?;
		}
		CdcMessage::ResolvedTs(_) => {
			// Resolved-ts watermarks are not yet consumed by SurrealDB; the
			// connector still emits them so the ingest path can adopt them
			// later without a wire-format change.
		}
		CdcMessage::Heartbeat(_) => {
			// Liveness ping; nothing to do beyond the trace below.
			trace!("cdc ingest heartbeat");
		}
	}
	Ok(())
}
