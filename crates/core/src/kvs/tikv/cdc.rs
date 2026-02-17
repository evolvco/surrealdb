//! CDC (Change Data Capture) consumer for TiKV-backed LIVE SELECT support.
//!
//! This module provides a background task that subscribes to TiKV's CDC stream
//! and sends LIVE SELECT notifications for any matching live queries.

use std::sync::Arc;
use std::time::Duration;

use async_channel::Sender;
use futures::StreamExt;
use reblessive::TreeStack;
use tikv::{CdcClient, CdcConfig, CdcEvent, CdcOptions, RowChange, RowOp};
use tokio::task::JoinHandle;

use super::cnf::{TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE, TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE};
use crate::dbs::{Action, Notification};
use crate::doc::process_cdc_event;
use crate::err::Error;
use crate::key::thing::Thing as ThingKey;
use crate::kvs::{KeyDecode, LockType, TransactionType};
use crate::sql::{Thing, Value};

use super::super::ds::TransactionFactory;

const TARGET: &str = "surrealdb::core::kvs::tikv::cdc";

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Handle to a running CDC consumer task.
pub struct CdcHandle {
	_handle: JoinHandle<()>,
}

/// CDC consumer that subscribes to TiKV changes and sends LIVE SELECT notifications.
pub struct CdcConsumer {
	pd_endpoint: String,
	sender: Sender<Notification>,
	transaction_factory: TransactionFactory,
}

impl CdcConsumer {
	/// Create a new CDC consumer.
	pub fn new(
		pd_endpoint: String,
		sender: Sender<Notification>,
		transaction_factory: TransactionFactory,
	) -> Self {
		Self {
			pd_endpoint,
			sender,
			transaction_factory,
		}
	}

	/// Spawn the CDC consumer as a background task.
	pub fn spawn(self) -> CdcHandle {
		let handle = tokio::spawn(async move {
			self.run().await;
		});
		CdcHandle {
			_handle: handle,
		}
	}

	/// Run the CDC consumer loop with automatic reconnection.
	async fn run(self) {
		let mut backoff = INITIAL_BACKOFF;

		loop {
			match self.run_inner().await {
				Ok(()) => {
					// Clean shutdown requested
					info!(target: TARGET, "CDC consumer shutting down");
					break;
				}
				Err(e) => {
					warn!(target: TARGET, "CDC consumer error: {}. Reconnecting in {:?}", e, backoff);
					tokio::time::sleep(backoff).await;
					// Exponential backoff with max
					backoff = (backoff * 2).min(MAX_BACKOFF);
				}
			}
		}
	}

	/// Inner run loop - returns Err on disconnection to trigger reconnect.
	async fn run_inner(&self) -> Result<(), Error> {
		info!(target: TARGET, "Connecting to TiKV CDC at {}", self.pd_endpoint);

		let max_decoding = *TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE;
		let max_encoding = *TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE;
		info!(
			target: TARGET,
			"CDC gRPC limits: max_decoding={} bytes ({}), max_encoding={} bytes ({})",
			max_decoding,
			if max_decoding == 0 { "unlimited".to_string() } else { format!("{}MB", max_decoding / 1024 / 1024) },
			max_encoding,
			if max_encoding == 0 { "unlimited".to_string() } else { format!("{}MB", max_encoding / 1024 / 1024) }
		);

		let cdc_config = CdcConfig::default()
			.with_max_decoding_message_size(max_decoding)
			.with_max_encoding_message_size(max_encoding);

		let cdc = CdcClient::new_with_cdc_config(
			vec![self.pd_endpoint.clone()],
			tikv::Config::default(),
			cdc_config,
		)
		.await
		.map_err(|e| Error::Ds(format!("Failed to create CDC client: {}", e)))?;

		info!(target: TARGET, "CDC client connected, subscribing to all changes");

		let mut stream = cdc
			.subscribe_all(CdcOptions::default())
			.await
			.map_err(|e| Error::Ds(format!("Failed to subscribe to CDC: {}", e)))?;

		info!(target: TARGET, "CDC subscription active, waiting for events");

		// Reset backoff on successful connection
		while let Some(event) = stream.next().await {
			match event {
				Ok(CdcEvent::Rows {
					rows,
					region_id,
				}) => {
					trace!(target: TARGET, "Received {} row changes from region {}", rows.len(), region_id);
					for row in rows {
						if let Err(e) = self.process_row(row).await {
							warn!(target: TARGET, "Failed to process CDC row: {}", e);
						}
					}
				}
				Ok(CdcEvent::ResolvedTs {
					..
				}) => {
					// Skip logging resolved timestamps - they're very frequent
				}
				Ok(CdcEvent::Error {
					region_id,
					error,
				}) => {
					warn!(target: TARGET, "CDC error for region {}: {}", region_id, error);
				}
				Ok(CdcEvent::Admin {
					region_id,
				}) => {
					debug!(target: TARGET, "CDC admin event for region {}", region_id);
				}
				Err(e) => {
					return Err(Error::Ds(format!("CDC stream error: {}", e)));
				}
			}
		}

		// Stream ended unexpectedly
		Err(Error::Ds("CDC stream ended unexpectedly".into()))
	}

	/// Process a single row change from CDC.
	async fn process_row(&self, row: RowChange) -> Result<(), Error> {
		// Try to decode as a record key, skip if not a record
		let key_bytes: Vec<u8> = row.key.into();
		let thing_key = match ThingKey::decode(&key_bytes) {
			Ok(k) => k,
			Err(_) => {
				// Not a record key (could be index, metadata, etc.) - skip silently
				return Ok(());
			}
		};

		let ns = thing_key.ns;
		let db = thing_key.db;
		let tb = thing_key.tb;
		let id = thing_key.id;

		// Determine action from operation type
		let action = match row.op {
			RowOp::Put => {
				if row.old_value.is_some() {
					Action::Update
				} else {
					Action::Create
				}
			}
			RowOp::Delete => Action::Delete,
			RowOp::Unknown => return Ok(()),
		};

		// Skip CDC events with no value for Create/Update
		// (these are internal bookkeeping like reference keys, not actual record data)
		if matches!(action, Action::Create | Action::Update) && row.value.is_none() {
			trace!(target: TARGET, "Skipping {:?} with no value for {}.{}.{}:{:?}", action, ns, db, tb, id);
			return Ok(());
		}

		trace!(target: TARGET, "Processing {:?} on {}.{}.{}:{:?}", action, ns, db, tb, id);

		// Decode the document values
		let current: Value = if let Some(ref val) = row.value {
			revision::from_slice(val).unwrap_or(Value::None)
		} else {
			Value::None
		};

		let initial: Value = if let Some(ref val) = row.old_value {
			revision::from_slice(val).unwrap_or(Value::None)
		} else {
			Value::None
		};

		// Build the record Thing
		let record = Thing::from((tb.to_string(), id));

		// Create a read transaction for the live query processing
		let tx = Arc::new(
			self.transaction_factory
				.transaction(TransactionType::Read, LockType::Optimistic)
				.await?,
		);

		// Process the CDC event using the shared live query logic
		let mut stack = TreeStack::new();
		stack
			.enter(|stk| {
				process_cdc_event(
					stk,
					tx,
					&self.sender,
					ns,
					db,
					tb,
					record,
					action,
					initial,
					current,
				)
			})
			.finish()
			.await?;

		Ok(())
	}
}
