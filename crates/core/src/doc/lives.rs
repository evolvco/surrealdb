use crate::ctx::{Context, MutableContext};
use crate::dbs::Action;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::doc::CursorDoc;
use crate::doc::CursorValue;
use crate::doc::Document;
use crate::err::Error;
use crate::idx::planner::RecordStrategy;
use crate::kvs::Transaction;
use crate::sql::Thing;
use crate::sql::paths::AC;
use crate::sql::paths::ID;
use crate::sql::paths::META;
use crate::sql::paths::RD;
use crate::sql::paths::TK;
use crate::sql::permission::Permission;
use crate::sql::statements::LiveStatement;
use crate::sql::{Idiom, Value};
use async_channel::Sender;
use reblessive::tree::Stk;
use std::sync::Arc;

impl Document {
	/// Processes any LIVE SELECT statements which
	/// have been defined for the table which this
	/// record belongs to. This functions loops
	/// through the live queries and processes them
	/// all within the currently running transaction.
	pub(super) async fn process_table_lives(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check import
		if opt.import {
			return Ok(());
		}

		// Check if we can send notifications
		let Some(chn) = opt.sender.as_ref() else {
			// no channel so nothing to do.
			return Ok(());
		};

		// For distributed datastores (TiKV, FoundationDB, SurrealCS),
		// live query notifications are handled by the CDC consumer.
		// Skip the local path to avoid duplicate notifications.
		if !ctx.tx().local() {
			return Ok(());
		}

		// Check if changed
		if !self.changed() {
			return Ok(());
		}

		// Get the record id of this document
		let rid = self.id.clone().ok_or(Error::Unreachable(
			"Processing live query for record without a Record ID".into(),
		))?;

		// Determine the action from the statement type
		let action = if stm.is_delete() {
			Action::Delete
		} else if self.is_new() {
			Action::Create
		} else {
			Action::Update
		};

		// Get the current and initial docs
		let current = self.current.doc.as_arc();
		let initial = self.initial.doc.as_arc();

		// Get namespace and database
		let ns = opt.ns()?;
		let db = opt.db()?;

		// Get all live queries for this table
		let lvs = self.lv(ctx, opt).await?;

		// Loop through all live query statements
		for lv in lvs.iter() {
			// Only process if this live query was created on this node
			// (for local datastores, each node handles its own live queries)
			if opt.id()? != lv.node.0 {
				continue;
			}

			// Process this live query
			self.process_lv(
				stk,
				ctx.tx(),
				chn,
				lv,
				rid.clone(),
				action.clone(),
				current.clone(),
				initial.clone(),
				ns,
				db,
				ctx.value("session"),
				Some(opt),
			)
			.await?;
		}

		Ok(())
	}

	/// Process a single live query for a document change.
	/// This is the core logic shared between local and CDC paths.
	async fn process_lv(
		&mut self,
		stk: &mut Stk,
		tx: Arc<Transaction>,
		sender: &Sender<Notification>,
		lv: &LiveStatement,
		rid: Arc<Thing>,
		action: Action,
		current: Arc<Value>,
		initial: Arc<Value>,
		ns: &str,
		db: &str,
		source_session: Option<&Value>,
		parent_opt: Option<&Options>,
	) -> Result<(), Error> {
		// Ensure that a session exists on the LIVE query
		let sess = match lv.session.as_ref() {
			Some(v) => v,
			None => return Ok(()),
		};

		// Ensure that auth info exists on the LIVE query
		let auth = match lv.auth.clone() {
			Some(v) => v,
			None => return Ok(()),
		};

		// Create the live query statement wrapper
		let lq = Statement::from(lv);

		// Get the event action string for context variables
		let event_name = match action {
			Action::Create => Value::from("CREATE"),
			Action::Update => Value::from("UPDATE"),
			Action::Delete => Value::from("DELETE"),
		};

		// Create context with session variables from the live query
		let mut lqctx = MutableContext::background();
		lqctx.set_transaction(tx);

		// Add session params for field projections and WHERE clauses
		lqctx.add_value("access", sess.pick(AC.as_ref()).into());
		lqctx.add_value("auth", sess.pick(RD.as_ref()).into());
		lqctx.add_value("token", sess.pick(TK.as_ref()).into());
		lqctx.add_value("session", sess.clone().into());

		// Add $before, $after, $value, and $event params
		lqctx.add_value("event", event_name.into());
		lqctx.add_value("value", current.clone());
		lqctx.add_value("after", current);
		lqctx.add_value("before", initial);

		let lqctx = lqctx.freeze();

		// Create options with auth from the live query
		let lqopt = match parent_opt {
			Some(opt) => opt.new_with_perms(true).with_auth(Arc::from(auth)),
			None => Options::new()
				.with_ns(Some(ns.into()))
				.with_db(Some(db.into()))
				.with_perms(true)
				.with_auth(Arc::from(auth)),
		};

		// Get the document to check against based on action
		let doc = match (self.check_reduction_required(&lqopt)?, action == Action::Delete) {
			(true, true) => {
				&self.compute_reduced_target(stk, &lqctx, &lqopt, &self.initial).await?
			}
			(true, false) => {
				&self.compute_reduced_target(stk, &lqctx, &lqopt, &self.current).await?
			}
			(false, true) => &self.initial,
			(false, false) => &self.current,
		};

		// Check WHERE clause
		match self.lq_check(stk, &lqctx, &lqopt, &lq, doc).await {
			Err(Error::Ignore) => return Ok(()),
			Err(e) => return Err(e),
			Ok(_) => (),
		}

		// Check permissions
		match self.lq_allow(stk, &lqctx, &lqopt, &lq).await {
			Err(Error::Ignore) => return Ok(()),
			Err(e) => return Err(e),
			Ok(_) => (),
		}

		// Compute the result based on action
		let mut result = match action {
			Action::Delete => {
				// For DELETE, compute the initial document
				let lqopt_futures = lqopt.new_with_futures(true);
				let mut result =
					doc.doc.as_ref().compute(stk, &lqctx, &lqopt_futures, Some(doc)).await?;
				// Remove metadata fields
				result.del(stk, &lqctx, &lqopt_futures, &*META).await?;
				result
			}
			_ => {
				// For CREATE/UPDATE, use lq_pluck for field projections
				match self.lq_pluck(stk, &lqctx, &lqopt, lv, doc).await {
					Err(Error::Ignore) => return Ok(()),
					Err(e) => return Err(e),
					Ok(v) => v,
				}
			}
		};

		// Process FETCH clause if present
		if let Some(fetchs) = &lv.fetch {
			let mut idioms = Vec::with_capacity(fetchs.0.len());
			for fetch in fetchs.iter() {
				fetch.compute(stk, &lqctx, &lqopt, &mut idioms).await?;
			}
			for i in &idioms {
				stk.run(|stk| result.fetch(stk, &lqctx, &lqopt, i)).await?;
			}
		}

		// Extract the source connection ID from the context that triggered this change
		let source_connection = source_session
			.map(|s| s.pick(&*ID))
			.filter(|v| !v.is_none_or_null())
			.map(|v| v.as_string());

		// Send the notification
		let notification = Notification::new(
			lv.id,
			action,
			Value::Thing(rid.as_ref().clone()),
			result,
			source_connection,
		);

		if sender.send(notification).await.is_err() {
			// Channel closed, stop processing
			return Ok(());
		}

		Ok(())
	}

	/// Check the WHERE clause for a LIVE query
	async fn lq_check(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		doc: &CursorDoc,
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = stm.cond() {
			// Check if the expression is truthy
			if !cond.compute(stk, ctx, opt, Some(doc)).await?.is_truthy() {
				// Ignore this document
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}

	/// Check any PERMISSIONS for a LIVE query
	async fn lq_allow(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Should we run permissions checks?
		if opt.check_perms(stm.into())? {
			// Get the table
			let tb = self.tb(ctx, opt).await?;
			// Process the table permissions
			match &tb.permissions.select {
				Permission::None => return Err(Error::Ignore),
				Permission::Full => return Ok(()),
				Permission::Specific(e) => {
					// Retrieve the document to check permissions against
					let doc = if stm.is_delete() {
						&self.initial
					} else {
						&self.current
					};

					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !e.compute(stk, ctx, opt, Some(doc)).await.is_ok_and(|x| x.is_truthy()) {
						return Err(Error::Ignore);
					}
				}
			}
		}
		// Carry on
		Ok(())
	}

	async fn lq_pluck(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &LiveStatement,
		doc: &CursorDoc,
	) -> Result<Value, Error> {
		if stm.expr.is_empty() {
			// lq_pluck is only called for CREATE and UPDATE events, so `doc` is the current document
			// We only need to retrieve the initial document to DIFF against the current document
			let initial = if self.check_reduction_required(opt)? {
				&self.compute_reduced_target(stk, ctx, opt, &self.initial).await?
			} else {
				&self.initial
			};

			Ok(initial.doc.as_ref().diff(doc.doc.as_ref(), Idiom::default()).into())
		} else {
			stm.expr.compute(stk, ctx, opt, Some(doc), false).await
		}
	}
}

/// Process a CDC event for live query notifications.
/// This is called by the CDC consumer for distributed datastores (TiKV, FoundationDB, etc.)
/// to process live query notifications for changes detected via change data capture.
pub async fn process_cdc_event(
	stk: &mut Stk,
	tx: Arc<Transaction>,
	sender: &Sender<Notification>,
	ns: &str,
	db: &str,
	tb: &str,
	rid: Thing,
	action: Action,
	initial: Value,
	current: Value,
) -> Result<(), Error> {
	// Get all live queries for this table
	let lvs = tx.all_tb_lives(ns, db, tb).await?;

	if lvs.is_empty() {
		return Ok(());
	}

	// Create a Document with the initial and current values
	let rid_arc = Arc::new(rid.clone());
	let current_arc = Arc::new(current.clone());
	let initial_arc = Arc::new(initial.clone());

	let mut doc = Document::new(
		Some(rid_arc.clone()),
		None,
		None,
		current_arc.clone(),
		Workable::Normal,
		false,
		RecordStrategy::KeysAndValues,
	);

	// Set the initial document value (different from current for UPDATE/DELETE)
	doc.initial = CursorDoc::new(Some(rid_arc.clone()), None, CursorValue::from(initial_arc));
	doc.initial_reduced = doc.initial.clone();

	// Process each live query (CDC processes ALL live queries, regardless of node)
	for lv in lvs.iter() {
		doc.process_lv(
			stk,
			tx.clone(),
			sender,
			lv,
			rid_arc.clone(),
			action.clone(),
			current_arc.clone(),
			Arc::new(initial.clone()),
			ns,
			db,
			None, // CDC events don't have source session - TiKV only stores data, not SurrealDB session metadata
			None, // CDC path has no parent Options - uses fresh Options with default auth_enabled
		)
		.await?;
	}

	Ok(())
}
