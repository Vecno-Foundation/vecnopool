//src/vecnod.rs

use anyhow::Result;
use log::warn;
use proto::vecnod_message::Payload;
use proto::submit_block_response_message::RejectReason;
pub use proto::RpcBlock;
use proto::*;
use rpc_client::RpcClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use http::Uri;
use std::convert::TryFrom;

pub type Send<T> = mpsc::UnboundedSender<T>;
type Recv<T> = mpsc::UnboundedReceiver<T>;

#[derive(Clone, Debug)]
pub struct VecnodHandle {
    pub send: Send<Payload>,
}

impl VecnodHandle {
    pub fn new() -> (Self, Recv<Payload>) {
        let (send, recv) = mpsc::unbounded_channel();
        (VecnodHandle { send }, recv)
    }

    pub fn submit_block(&self, block: RpcBlock) {
        let _ = self.send.send(Payload::submit_block(block, false));
    }

    pub fn send_cmd(&self, payload: Payload) {
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
}

struct ClientTask {
    url: String,
    send_msg: Send<Message>,
    recv_cmd: Recv<Payload>,
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

        while let Some(VecnodMessage { payload }) = stream.message().await? {
            let msg = match payload {
                Some(Payload::GetInfoResponse(info)) => Message::Info {
                    version: info.server_version,
                },
                Some(Payload::GetBlockDagInfoResponse(info)) => Message::BlockDagInfo {
                    virtual_daa_score: info.virtual_daa_score,
                },
                Some(Payload::SubmitBlockResponse(res)) => {
                    let res = match (RejectReason::try_from(res.reject_reason), res.error) {
                        (Ok(RejectReason::None), None) => None,
                        (Ok(_), Some(e)) => Some(e.message.into_boxed_str()),
                        (Err(_), Some(e)) => Some(e.message.into_boxed_str()),
                        _ => Some(Box::from("Unknown error")),
                    };
                    Message::SubmitBlockResult(res)
                },
                Some(Payload::GetBlockTemplateResponse(res)) => {
                    if let Some(e) = res.error {
                        warn!("Error in GetBlockTemplateResponse: {}", e.message);
                        continue;
                    }
                    if let Some(block) = res.block {
                        if block.header.is_none() {
                            warn!("Template block is missing a header");
                            continue;
                        }
                        Message::Template(block)
                    } else {
                        continue;
                    }
                },
                Some(Payload::NotifyNewBlockTemplateResponse(res)) => match res.error {
                    Some(e) => {
                        warn!("Unable to subscribe to new templates: {}", e.message);
                        Message::NewTemplate
                    }
                    None => {
                        Message::NewTemplate
                    }
                },
                Some(Payload::NewBlockTemplateNotification(_)) => {
                    Message::NewTemplate
                },
                Some(Payload::NotifyBlockAddedResponse(res)) => match res.error {
                    Some(e) => {
                        warn!("Error in NotifyBlockAddedResponse: {}", e.message);
                        continue;
                    }
                    None => {
                        Message::NewBlock
                    }
                },
                Some(Payload::BlockAddedNotification(_)) => {
                    Message::NewBlock
                },
                Some(payload) => {
                    warn!("Received unhandled message type: {:?}", payload);
                    continue;
                },
                None => {
                    warn!("Received empty payload");
                    continue;
                },
            };
            self.send_msg.send(msg)?;
        }

        warn!("Vecnod connection closed, attempting to reconnect");
        Ok(())
    }
}

#[derive(Clone)]
pub struct Client {
    pay_address: String,
    extra_data: String,
    send_cmd: Send<Payload>,
}

impl Client {
    pub fn new(
        url: &str,
        pay_address: &str,
        extra_data: &str,
        handle: VecnodHandle,
        recv_cmd: Recv<Payload>,
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
        let task = ClientTask {
            url,
            send_msg,
            recv_cmd,
        };

        tokio::spawn(async move {
            match task.run().await {
                Ok(_) => warn!("Vecnod connection closed"),
                Err(e) => warn!("Vecnod connection closed: {e}"),
            }
        });

        let send_cmd = handle.send.clone();
        send_cmd.send(Payload::get_info()).unwrap();
        send_cmd.send(Payload::notify_new_block_template()).unwrap();
        send_cmd.send(Payload::notify_block_added()).unwrap();

        let client = Client {
            pay_address,
            extra_data: extra_data.into(),
            send_cmd,
        };
        client.request_template();
        (client, recv_msg)
    }

    pub fn request_template(&self) -> bool {
        self.send_cmd
            .send(Payload::get_block_template(
                &self.pay_address,
                &self.extra_data,
            ))
            .is_ok()
    }
}

pub mod proto {
    use crate::pow;
    use crate::uint::U256;
    use crate::vecnod::Payload;
    use anyhow::Result;
    use blake3::Hash as Blake3Hash;
    use blake3::Hasher as Blake3State;

    const BLOCK_HASH_DOMAIN: &[u8; 32] = b"BlockHash\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

    include!(concat!(env!("OUT_DIR"), "/protowire.rs"));

    impl Payload {
        pub fn get_info() -> Self {
            Payload::GetInfoRequest(GetInfoRequestMessage {})
        }

        pub fn get_block_dag_info() -> Self {
            Payload::GetBlockDagInfoRequest(GetBlockDagInfoRequestMessage {})
        }

        pub fn submit_block(block: RpcBlock, allow_non_daa_blocks: bool) -> Self {
            Payload::SubmitBlockRequest(SubmitBlockRequestMessage {
                block: Some(block),
                allow_non_daa_blocks,
            })
        }

        pub fn get_block_template(pay_address: &str, extra_data: &str) -> Self {
            Payload::GetBlockTemplateRequest(GetBlockTemplateRequestMessage {
                pay_address: pay_address.into(),
                extra_data: extra_data.into(),
            })
        }

        pub fn notify_new_block_template() -> Self {
            Payload::NotifyNewBlockTemplateRequest(NotifyNewBlockTemplateRequestMessage {})
        }

        pub fn notify_block_added() -> Self {
            Payload::NotifyBlockAddedRequest(NotifyBlockAddedRequestMessage {})
        }
    }

    impl RpcBlockHeader {
        pub fn difficulty(&self) -> u64 {
            let target = pow::u256_from_compact_target(self.bits);
            pow::difficulty(target)
        }

        pub fn pre_pow(&self) -> Result<U256> {
            let hash = self.hash(true)?;
            let mut out = [0; 4];
            for (o, c) in out.iter_mut().zip(hash.as_bytes().chunks_exact(8)) {
                *o = u64::from_le_bytes(c.try_into().unwrap());
            }
            Ok(out.into())
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

            let (timestamp, nonce) = if pre_pow {
                (0, 0)
            } else {
                (self.timestamp, self.nonce)
            };

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