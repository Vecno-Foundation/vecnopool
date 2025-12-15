// src/vecnod.rs

use anyhow::Result;
use log::{debug, warn};
use proto::vecnod_message::Payload as RequestPayload;
use proto::vecnod_response::Payload as ResponsePayload;
use proto::submit_block_response_message::RejectReason;
use proto::RpcNotifyCommand;
pub use proto::RpcBlock;
use proto::*;
use rpc_client::RpcClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use http::Uri;
use std::convert::TryFrom;
use std::sync::Arc;
use crate::database::db::Db;
use tokio::sync::oneshot;
use std::collections::HashMap;

pub type Send<T> = mpsc::UnboundedSender<T>;
type Recv<T> = mpsc::UnboundedReceiver<T>;

#[derive(Clone, Debug)]
pub struct VecnodHandle {
    pub send: Send<RequestPayload>,
    pub block_status_senders: Arc<tokio::sync::Mutex<HashMap<String, oneshot::Sender<bool>>>>,
}

impl VecnodHandle {
    pub fn new() -> (Self, Recv<RequestPayload>) {
        let (send, recv) = mpsc::unbounded_channel();
        let block_status_senders = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        (
            VecnodHandle {
                send,
                block_status_senders,
            },
            recv,
        )
    }

    pub fn submit_block(&self, block: RpcBlock) {
        let _ = self.send.send(RequestPayload::SubmitBlockRequest(SubmitBlockRequestMessage {
            block: Some(block),
            allow_non_daa_blocks: false,
        }));
    }

    pub fn send_cmd(&self, payload: RequestPayload) {
        let _ = self.send.send(payload);
    }
}

#[derive(Debug)]
pub enum Message {
    Info { version: String },
    BlockDagInfo { virtual_daa_score: u64 },
    Template(RpcBlock),
    NewTemplate,
    SubmitBlockResult(Option<Box<str>>),
    NewBlock,
    BlockStatus,
}

struct ClientTask {
    url: String,
    send_msg: Send<Message>,
    recv_cmd: Recv<RequestPayload>,
    db: Arc<Db>,
    pool_address: String,
    block_status_senders: Arc<tokio::sync::Mutex<HashMap<String, oneshot::Sender<bool>>>>,
}

impl ClientTask {
    async fn run(self) -> Result<()> {
        let uri: Uri = self.url.parse().map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;
        let mut client = RpcClient::connect(uri).await?;
        let mut stream = client
            .message_stream(
                UnboundedReceiverStream::new(self.recv_cmd)
                    .map(|p| VecnodMessage { payload: Some(p) }),
            )
            .await?
            .into_inner();

        while let Some(VecnodResponse { id: _, payload }) = stream.message().await? {
            let msg = match payload {
                Some(ResponsePayload::GetInfoResponse(info)) => Message::Info {
                    version: info.server_version,
                },
                Some(ResponsePayload::GetBlockDagInfoResponse(info)) => Message::BlockDagInfo {
                    virtual_daa_score: info.virtual_daa_score,
                },
                Some(ResponsePayload::SubmitBlockResponse(res)) => {
                    let res = match (RejectReason::try_from(res.reject_reason), res.error) {
                        (Ok(RejectReason::None), None) => None,
                        (Ok(_), Some(e)) => Some(e.message.into_boxed_str()),
                        (Err(_), Some(e)) => Some(e.message.into_boxed_str()),
                        _ => Some(Box::from("Unknown error")),
                    };
                    Message::SubmitBlockResult(res)
                },
                Some(ResponsePayload::GetBlockTemplateResponse(res)) => {
                    if let Some(e) = res.error {
                        warn!("Error in GetBlockTemplateResponse: {}", e.message);
                        continue;
                    }
                    if let Some(block) = res.block {
                        if block.header.is_none() {
                            warn!("Template block is missing a header");
                            continue;
                        }
                        debug!("Fresh block template received from node (triggered by notification or initial request)");
                        Message::Template(block)
                    } else {
                        warn!("GetBlockTemplateResponse contained no block");
                        continue;
                    }
                },
                Some(ResponsePayload::NotifyNewBlockTemplateResponse(res)) => match res.error {
                    Some(e) => {
                        warn!("Unable to subscribe to new templates: {}", e.message);
                        Message::NewTemplate
                    }
                    None => {
                        debug!("Successfully subscribed to NewBlockTemplate notifications");
                        Message::NewTemplate
                    }
                },
                Some(ResponsePayload::NewBlockTemplateNotification(_)) => {
                    debug!("NewBlockTemplate notification received — a fresh template is available!");
                    Message::NewTemplate
                },
                Some(ResponsePayload::NotifyBlockAddedResponse(res)) => match res.error {
                    Some(e) => {
                        warn!("Error in NotifyBlockAddedResponse: {}", e.message);
                        continue;
                    }
                    None => {
                        debug!("Successfully subscribed to BlockAdded notifications");
                        Message::NewBlock
                    }
                },
                Some(ResponsePayload::BlockAddedNotification(block_added)) => {
                    if let Some(block) = block_added.block {
                        // Existing reward detection logic (unchanged)
                        let coinbase_txs: Vec<_> = block
                            .transactions
                            .iter()
                            .filter(|tx| tx.inputs.is_empty())
                            .collect();
                        let matches_pool_address = coinbase_txs.iter().any(|tx| {
                            tx.outputs.iter().any(|output| {
                                output
                                    .verbose_data
                                    .as_ref()
                                    .map(|vd| vd.script_public_key_address == self.pool_address)
                                    .unwrap_or(false)
                            })
                        });

                        if matches_pool_address {
                            let block_hash = block
                                .verbose_data
                                .as_ref()
                                .map(|vd| vd.hash.clone())
                                .unwrap_or_default();
                            if coinbase_txs.len() > 1 {
                                // existing multi-coinbase logging
                                let tx_ids: Vec<_> = coinbase_txs
                                    .iter()
                                    .filter_map(|tx| tx.verbose_data.as_ref().map(|vd| vd.transaction_id.clone()))
                                    .collect();
                                let amounts: Vec<_> = coinbase_txs.iter().map(|tx| {
                                    tx.outputs.iter().filter(|output| {
                                        output.verbose_data.as_ref().map(|vd| vd.script_public_key_address == self.pool_address).unwrap_or(false)
                                    }).map(|output| output.amount).sum::<u64>()
                                }).collect();
                                debug!(
                                    "Multiple coinbase-like transactions found in block {}: count={}, transaction_ids={:?}, amounts={:?}, total_amount={}",
                                    block_hash, coinbase_txs.len(), tx_ids, amounts, amounts.iter().sum::<u64>()
                                );
                            } else {
                                debug!("Block with matching pool address found in coinbase: {:?}", block_hash);
                            }

                            if let Some(header) = &block.header {
                                // existing DB recording logic
                                let reward_block_hash = block_hash.clone();
                                let job_id = 0;
                                let extranonce = "";
                                let nonce = format!("{:016x}", header.nonce);
                                let daa_score = header.daa_score;
                                let pool_wallet = self.pool_address.clone();
                                let amount = coinbase_txs.iter().filter(|tx| tx.inputs.is_empty()).flat_map(|tx| {
                                    tx.outputs.iter().filter(|output| {
                                        output.verbose_data.as_ref().map(|vd| vd.script_public_key_address == self.pool_address).unwrap_or(false)
                                    })
                                }).map(|output| output.amount).sum::<u64>();
                                let is_chain_block = block.verbose_data.as_ref().map(|vd| vd.is_chain_block).unwrap_or(false);

                                if let Err(e) = self.db.add_block_details(
                                    &reward_block_hash,
                                    &reward_block_hash,
                                    job_id,
                                    extranonce,
                                    &nonce,
                                    daa_score,
                                    &pool_wallet,
                                    amount,
                                    is_chain_block,
                                ).await {
                                    warn!("Failed to add block details to database: {:?}", e);
                                } else {
                                    debug!(
                                        "Successfully added block details to database: hash = {}, daaScore = {}, amount = {}",
                                        reward_block_hash, daa_score, amount
                                    );
                                }
                            }
                        } else {
                            debug!("No coinbase transactions match pool address");
                        }
                    }
                    Message::NewBlock
                },
                Some(ResponsePayload::GetBlockResponse(res)) => {
                    // existing block status handling
                    let block_hash = res.block.as_ref().and_then(|b| b.verbose_data.as_ref().map(|vd| vd.hash.clone())).unwrap_or_default();
                    let is_chain_block = res.block.as_ref().and_then(|b| b.verbose_data.as_ref().map(|vd| vd.is_chain_block)).unwrap_or(false);

                    let mut senders = self.block_status_senders.lock().await;
                    if let Some(sender) = senders.remove(&block_hash) {
                        let _ = sender.send(is_chain_block);
                    }

                    if res.error.is_some() {
                        warn!("Error in GetBlockResponse for {}: {:?}", block_hash, res.error);
                    }

                    Message::BlockStatus
                },
                Some(payload) => {
                    warn!("Received unhandled response type: {:?}", payload);
                    continue;
                },
                None => {
                    warn!("Received empty payload");
                    continue;
                },
            };
            let _ = self.send_msg.send(msg);
        }

        warn!("Vecnod connection closed, attempting to reconnect");
        Ok(())
    }
}

#[derive(Clone)]
pub struct Client {
    pay_address: String,
    extra_data: String,
    send_cmd: Send<RequestPayload>,
}

impl Client {
    pub fn new(
        url: &str,
        pay_address: &str,
        extra_data: &str,
        handle: VecnodHandle,
        recv_cmd: Recv<RequestPayload>,
        db: Arc<Db>,
        pool_address: String,
    ) -> (Self, Recv<Message>) {
        let (send_msg, recv_msg) = mpsc::unbounded_channel();

        let pay_address = if !pay_address.starts_with("vecno") {
            format!("vecno:{}", pay_address)
        } else {
            pay_address.into()
        };

        let url = if !url.starts_with("http") {
            format!("http://{}", url)
        } else {
            url.into()
        };
        let block_status_senders = handle.block_status_senders.clone();
        let task = ClientTask {
            url,
            send_msg,
            recv_cmd,
            db,
            pool_address,
            block_status_senders,
        };

        tokio::spawn(async move {
            if let Err(e) = task.run().await {
                warn!("Vecnod client task failed: {}", e);
            }
        });

        let send_cmd = handle.send.clone();
        let _ = send_cmd.send(RequestPayload::GetInfoRequest(GetInfoRequestMessage {}));
        let _ = send_cmd.send(RequestPayload::NotifyNewBlockTemplateRequest(
            NotifyNewBlockTemplateRequestMessage {
                command: RpcNotifyCommand::NotifyStart as i32,
            },
        ));
        let _ = send_cmd.send(RequestPayload::NotifyBlockAddedRequest(
            NotifyBlockAddedRequestMessage {
                command: RpcNotifyCommand::NotifyStart as i32,
            },
        ));

        let client = Client {
            pay_address,
            extra_data: extra_data.into(),
            send_cmd,
        };
        let _ = client.request_template();  // Initial template
        (client, recv_msg)
    }

    pub fn request_template(&self) -> bool {
        self.send_cmd
            .send(RequestPayload::GetBlockTemplateRequest(
                GetBlockTemplateRequestMessage {
                    pay_address: self.pay_address.clone(),
                    extra_data: self.extra_data.clone(),
                },
            ))
            .is_ok()
    }
}

pub mod proto {
    use crate::pow;
    use primitive_types::U256;
    use anyhow::Result;
    use blake3::Hash as Blake3Hash;
    use blake3::Hasher as Blake3State;

    const BLOCK_HASH_DOMAIN: &[u8; 32] = b"BlockHash\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

    include!(concat!(env!("OUT_DIR"), "/protowire.rs"));

    #[allow(dead_code)]
    impl vecnod_message::Payload {
        pub fn get_info() -> Self { vecnod_message::Payload::GetInfoRequest(GetInfoRequestMessage {}) }
        pub fn get_block_dag_info() -> Self { vecnod_message::Payload::GetBlockDagInfoRequest(GetBlockDagInfoRequestMessage {}) }
        pub fn submit_block(block: RpcBlock, allow_non_daa_blocks: bool) -> Self {
            vecnod_message::Payload::SubmitBlockRequest(SubmitBlockRequestMessage { block: Some(block), allow_non_daa_blocks })
        }
        pub fn get_block_template(pay_address: &str, extra_data: &str) -> Self {
            vecnod_message::Payload::GetBlockTemplateRequest(GetBlockTemplateRequestMessage {
                pay_address: pay_address.into(),
                extra_data: extra_data.into(),
            })
        }
        pub fn notify_new_block_template() -> Self {
            vecnod_message::Payload::NotifyNewBlockTemplateRequest(NotifyNewBlockTemplateRequestMessage {
                command: RpcNotifyCommand::NotifyStart as i32,
            })
        }
        pub fn notify_block_added() -> Self {
            vecnod_message::Payload::NotifyBlockAddedRequest(NotifyBlockAddedRequestMessage {
                command: RpcNotifyCommand::NotifyStart as i32,
            })
        }
    }

    impl RpcBlockHeader {
        pub fn difficulty(&self) -> u64 {
            let target = pow::u256_from_compact_target_bits(self.bits);
            pow::difficulty(target)
        }

        pub fn pre_pow(&self) -> Result<U256> {
            let hash = self.hash(true)?;
            let mut out = [0; 4];
            for (o, c) in out.iter_mut().zip(hash.as_bytes().chunks_exact(8)) {
                *o = u64::from_le_bytes(c.try_into().unwrap());
            }
            Ok(U256(out))
        }

        pub fn hash(&self, pre_pow: bool) -> Result<Blake3Hash> {
            let mut state = Blake3State::new_keyed(BLOCK_HASH_DOMAIN);

            let version = self.version as u16;
            state.update(&version.to_le_bytes());
            let mut parents = self.parents.len() as u64;
            state.update(&parents.to_le_bytes());

            let mut hash = [0u8; 32];
            for parent in &self.parents {
                parents = parent.parent_hashes.len() as u64;
                state.update(&parents.to_le_bytes());
                for h in &parent.parent_hashes {
                    hex::decode_to_slice(h, &mut hash)?;
                    state.update(&hash);
                }
            }
            hex::decode_to_slice(&self.hash_merkle_root, &mut hash)?;
            state.update(&hash);
            hex::decode_to_slice(&self.accepted_id_merkle_root, &mut hash)?;
            state.update(&hash);
            hex::decode_to_slice(&self.utxo_commitment, &mut hash)?;
            state.update(&hash);

            let (timestamp, nonce) = if pre_pow { (0, 0) } else { (self.timestamp, self.nonce) };

            state
                .update(&timestamp.to_le_bytes())
                .update(&self.bits.to_le_bytes())
                .update(&nonce.to_le_bytes())
                .update(&self.daa_score.to_le_bytes())
                .update(&self.blue_score.to_le_bytes());

            let len = (self.blue_work.len() + 1) / 2;
            if self.blue_work.len() % 2 == 0 {
                hex::decode_to_slice(&self.blue_work, &mut hash[..len])?;
            } else {
                hex::decode_to_slice(format!("0{}", self.blue_work), &mut hash[..len])?;
            }
            state
                .update(&(len as u64).to_le_bytes())
                .update(&hash[..len]);

            hex::decode_to_slice(&self.pruning_point, &mut hash)?;
            state.update(&hash);

            let hash = state.finalize();
            Ok(hash)
        }
    }
}