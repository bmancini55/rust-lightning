//! Gossip related stuff


use util::logger::Logger;
use util::events;

use ln::msgs::GossipQueriesHandler;
use ln::msgs::{QueryChannelRange, ReplyChannelRange};
use ln::msgs::{QueryShortChannelIds, ReplyShortChannelIdsEnd};

use bitcoin::secp256k1::PublicKey;
use bitcoin::hash_types::BlockHash;

use std::ops::Deref;
use std::sync::Mutex;
use std::sync::RwLock;


/// TODO bmancini
pub struct SimplGossipQueryHandler<L: Deref>
where L::Target: Logger
{
	sync_tasks: RwLock<Vec<GossipSyncTask>>,
	pending_events: Mutex<Vec<events::MessageSendEvent>>,
	logger: L,
}

impl<L: Deref> SimplGossipQueryHandler<L>
where L::Target: Logger
{
	/// TODO bmancini
	pub fn new(logger: L) -> Self {
		Self{
			sync_tasks: RwLock::new(vec![]),
			pending_events: Mutex::new(vec![]),
			logger,
		}
	}

	fn send_channels_query(&self, task: &mut GossipSyncTask) {

		const MAX_SCID_SIZE: usize = 8000;
		let scid_size = std::cmp::min(task.short_channel_ids.len(), MAX_SCID_SIZE);

		log_trace!(self.logger, "Sending channel query to {} for {} channels", log_pubkey!(task.node_id), scid_size);

		let mut short_channel_ids: Vec<u64> = Vec::with_capacity(scid_size);

		for scid in task.short_channel_ids.drain(..scid_size) {
			short_channel_ids.push(scid);
		}

		// enqueue the message to the peer
		let mut pending_events = self.pending_events.lock().unwrap();
		pending_events.push(events::MessageSendEvent::SendChannelsQuery {
			node_id: task.node_id.clone(),
			msg: QueryShortChannelIds {
				chain_hash: task.chain_hash.clone(),
				short_channel_ids,
			}
		});
	}
}

impl<L: Deref> events::MessageSendEventsProvider for SimplGossipQueryHandler<L>
where L::Target: Logger {
	fn get_and_clear_pending_msg_events(&self) -> Vec<events::MessageSendEvent> {
		let mut ret = Vec::new();
		let mut pending_events = self.pending_events.lock().unwrap();
		std::mem::swap(&mut ret, &mut pending_events);
		ret
	}
}

impl<L: Deref + Sync + Send> GossipQueriesHandler for SimplGossipQueryHandler<L>
where L::Target: Logger {
	fn query_range(&self, their_node_id: &PublicKey, chain_hash: BlockHash, first_blocknum: u32, number_of_blocks: u32) {
		let sync_task = GossipSyncTask::new(their_node_id, chain_hash, first_blocknum, number_of_blocks);
		self.sync_tasks.write().unwrap().push(sync_task);

		log_trace!(self.logger, "Sending channel range query to {} starting with block {} for {} blocks", log_pubkey!(their_node_id), first_blocknum, number_of_blocks);

		let mut pending_events = self.pending_events.lock().unwrap();
		pending_events.push(events::MessageSendEvent::SendChannelRangeQuery {
			node_id: their_node_id.clone(),
			msg: QueryChannelRange {
				chain_hash,
				first_blocknum,
				number_of_blocks,
			},
		});
	}

	fn handle_reply_channel_range(&self, their_node_id: &PublicKey, msg: &ReplyChannelRange) {
		let mut sync_tasks_lock = self.sync_tasks.write().unwrap();

		let mut blah: Option<&mut GossipSyncTask> = None;
		for task in sync_tasks_lock.iter_mut() {
			if task.node_id == *their_node_id {
				blah = Some(task);
			}
		}

		// TODO bmancini - how do we handle unexplained messages?
		if blah.is_none() {
			return;
		}

		let task = blah.unwrap();

		log_trace!(self.logger, "Received reply_channel_range from {} with scids={} full_information={}", log_pubkey!(their_node_id), msg.short_channel_ids.len(), msg.full_information);

		// TODO bmancini - how do we fail the query
		if !msg.full_information && msg.short_channel_ids.len() == 0 {
			// fail the query
		}

		// Add the short_channel_ids from the query into a request. This
		// is not efficient at all, but will be addressed
		// TODO bmancini - make more efficient
		for short_channel_id in msg.short_channel_ids.iter() {
			task.short_channel_ids.push(*short_channel_id);
		}

		// check if we have reached the of this particular query
		let last_blocknum = match (msg.first_blocknum).checked_add(msg.number_of_blocks) {
			Some(val) => val,
			None => u32::MAX,
		};

		//  once we have received all of the scids, we query the chunk
		let ready_for_channels_query = last_blocknum == task.last_blocknum() && msg.full_information;
		if ready_for_channels_query {
			self.send_channels_query(task);
		}
	}

	fn handle_reply_short_channel_ids_end(&self, their_node_id: &PublicKey, msg: &ReplyShortChannelIdsEnd) {
		let mut sync_tasks_lock = self.sync_tasks.write().unwrap();

		let mut blah: Option<&mut GossipSyncTask> = None;
		for task in sync_tasks_lock.iter_mut() {
			if task.node_id == *their_node_id {
				blah = Some(task);
			}
		}

		log_trace!(self.logger, "Received reply_short_channel_ids_end from {} with full_information={}", log_pubkey!(their_node_id), msg.full_information);


		// TODO bmancini - how do we handle unexplained messages?
		if blah.is_none() {
			return;
		}

		let task = blah.unwrap();

		// TODO bmancini - how do we fail the query
		if !msg.full_information {
			// fail the query
		}

		// if we still have scids, issue an other query
		log_trace!(self.logger, "Remaining scids to process {}", task.short_channel_ids.len());
		if task.short_channel_ids.len() > 0 {
			self.send_channels_query(task);
		}
	}
}

/// TODO bmancini
pub struct GossipSyncTask {
	node_id: PublicKey,
	chain_hash: BlockHash,
	first_blocknum: u32,
	number_of_blocks: u32,
	short_channel_ids: Vec<u64>,
}

impl GossipSyncTask {
	/// TODO bmancini
	pub fn new(their_node_id: &PublicKey, chain_hash: BlockHash, first_blocknum: u32, number_of_blocks: u32) -> Self {
		Self {
			node_id: their_node_id.clone(),
			chain_hash,
			first_blocknum,
			number_of_blocks,
			short_channel_ids: vec![],
		}
	}

	/// TODO bmancini
	pub fn last_blocknum(&self) -> u32 {
		match self.first_blocknum.checked_add(self.number_of_blocks) {
			Some(val) => val,
			None => u32::MAX,
		}
	}
}
