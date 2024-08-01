//! Means of communicating with the network.

use crate::result::{self, Result};

use crate::record::{Record, Transfer, Vote};

use std::str::FromStr;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use solana_client::{rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, UiInstruction, UiMessage,
    UiParsedInstruction, UiTransactionEncoding,
};

/// Dig data to decompose the vote instruction, and send it to the channel.
/// Skip silently if not really a vote.
async fn emit_vote(
    tx: &mpsc::Sender<Record>,
    signature: &Signature,
    block_index: &u64,
    timestamp: &u64,
    data: &serde_json::Value,
) -> Result<()> {
    let serde_json::Value::Object(data) = data else {
        return Ok(());
    };
    let Some(serde_json::Value::Object(info)) = data.get("info") else {
        return Ok(());
    };
    let Some(serde_json::Value::String(vote_account)) = info.get("voteAccount") else {
        return Ok(());
    };
    let Some(serde_json::Value::String(vote_authority)) = info.get("voteAuthority") else {
        return Ok(());
    };

    let vote_account = Pubkey::from_str(vote_account)?;
    let vote_authority = Pubkey::from_str(vote_authority)?;

    let sent = tx
        .send(Record::Vote(Vote {
            signature: *signature,
            block_index: block_index.to_owned(),
            timestamp: timestamp.to_owned(),
            author: vote_authority,
            target: vote_account,
        }))
        .await;
    if let Err(e) = sent {
        tracing::trace!("While sending a vote: {e:?}");
    }

    Ok(())
}

/// Dig data to decompose the transfer instruction, and send it to the channel.
/// Skip silently if not really a transfer.
async fn emit_transfer(
    tx: &mpsc::Sender<Record>,
    signature: &Signature,
    block_index: &u64,
    timestamp: &u64,
    data: &serde_json::Value,
) -> Result<()> {
    let serde_json::Value::Object(data) = data else {
        return Ok(());
    };
    let Some(serde_json::Value::Object(info)) = data.get("info") else {
        return Ok(());
    };
    let Some(serde_json::Value::String(source)) = info.get("source") else {
        return Ok(());
    };
    let Some(serde_json::Value::String(destination)) = info.get("destination") else {
        return Ok(());
    };
    let Some(serde_json::Value::Number(lamports)) = info.get("lamports") else {
        return Ok(());
    };

    let source = Pubkey::from_str(source)?;
    let destination = Pubkey::from_str(destination)?;
    let lamports = lamports
        .as_u64()
        .ok_or_else(|| result::Error::SolanaBadNumber(lamports.to_string()))?;

    let sent = tx
        .send(Record::Transfer(Transfer {
            signature: *signature,
            block_index: block_index.to_owned(),
            timestamp: timestamp.to_owned(),
            source,
            destination,
            lamports,
        }))
        .await;
    if let Err(e) = sent {
        tracing::trace!("While sending a vote: {e:?}");
    }

    Ok(())
}

/// Record all the transactions contained in a given block.
/// This expects the block to be loaded with `UiTransactionEncoding::JsonParsed`.
async fn extract_transactions(
    tx: &mpsc::Sender<Record>,
    block_index: &u64,
    block_time: &u64,
    transactions: &[EncodedTransactionWithStatusMeta],
) -> Result<()> {
    for transaction in transactions {
        let transaction = match &transaction.transaction {
            // Encoding variant is set by the requestor,
            // so any other branch means the RPC did not abide by the spec.
            EncodedTransaction::Json(transaction) => transaction,
            transaction => {
                tracing::warn!("Skipping improperly encoded transaction: {transaction:?}");
                continue;
            }
        };
        // The first signature uniquely identifies the transaction.
        let main_signature = Signature::from_str(&transaction.signatures[0]);
        let main_signature = match main_signature {
            Err(e) => {
                tracing::warn!("Skipping transaction with less than one signature: {e:?}");
                continue;
            }
            Ok(main_signature) => main_signature,
        };
        let message = match &transaction.message {
            UiMessage::Parsed(message) => message,
            message => {
                tracing::warn!("Skipping transaction with bad message: {message:?}");
                continue;
            }
        };
        for instruction in &message.instructions {
            let instruction = match instruction {
                UiInstruction::Parsed(UiParsedInstruction::Parsed(instruction)) => instruction,
                _ => {
                    // Skipping partially decoded instructions silently.
                    continue;
                }
            };

            // We're only interested in vote and transfer instructions.
            match instruction.program_id.as_str() {
                "Vote111111111111111111111111111111111111111" => {
                    emit_vote(
                        tx,
                        &main_signature,
                        block_index,
                        block_time,
                        &instruction.parsed,
                    )
                    .await?
                }
                "11111111111111111111111111111111" => {
                    emit_transfer(
                        tx,
                        &main_signature,
                        block_index,
                        block_time,
                        &instruction.parsed,
                    )
                    .await?
                }
                _ => {
                    // If unsupported instruction, skipping it silently.
                    continue;
                }
            }
        }
    }
    Ok(())
}

/// Load the block and get all the transactions in it.
#[instrument(name = "extract", level = "info", skip(client, tx))]
async fn extract_all_transactions_in_block(
    tx: &mpsc::Sender<Record>,
    client: &RpcClient,
    block: u64,
) -> Result<()> {
    use solana_client::client_error::{ClientError, ClientErrorKind};
    use solana_client::rpc_request::RpcError::RpcResponseError;
    use solana_sdk::commitment_config::CommitmentConfig;

    tracing::info!("Extracting block #{block}...");
    let block_data = client.get_block_with_config(
        block,
        RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
            ..Default::default()
        },
    );
    tracing::trace!("Loaded block data");
    let block_data = match block_data {
        Err(ClientError {
            kind: ClientErrorKind::RpcError(RpcResponseError { code: -32007, .. }),
            ..
        }) => {
            // This is benign, and we don't want to pollute the logs with it.
            tracing::info!("Block #{block} is missing, skipping...");
            return Ok(());
        }
        Err(e) => {
            tracing::error!("Failed to get block #{block}: {e:?}, skipping...");
            return Ok(());
        }
        Ok(block_data) => block_data,
    };
    let block_time = client.get_block_time(block).map(|t| t as u64)?;
    tracing::trace!("Block #{block} was mined at {block_time}");
    let Some(transactions) = block_data.transactions else {
        tracing::warn!("Block #{block} has no transactions, skipping...");
        return Ok(());
    };
    extract_transactions(tx, &block, &block_time, &transactions).await
}

/// [extract_continuously] sans retries.
async fn do_extract_continuously(
    tx: &mpsc::Sender<Record>,
    stop: CancellationToken,
    rpc_url: &str,
    since_block: &mut Option<u64>,
) -> Result<()> {
    let client = RpcClient::new(rpc_url);
    tracing::info!("Connected to `{}`", client.url());

    let mut next_block = match since_block {
        None => {
            let epoch_schedule = client.get_epoch_schedule()?;
            let current_epoch = client.get_epoch_info()?.epoch;
            epoch_schedule.get_first_slot_in_epoch(current_epoch)
        }
        Some(block) => *block,
    };

    tracing::info!("Starting with block #{next_block}...");

    loop {
        extract_all_transactions_in_block(tx, &client, next_block).await?;

        if stop.is_cancelled() {
            break Ok(());
        }

        next_block += 1;
        *since_block = Some(next_block);
    }
}

/// Connect to the provided RPC URL and extract all the transaction data for the current epoch
/// and onwards, sending them by the channel.
/// Stop if there are no readily available finalized blocks.
/// Retry up to 3 times if anything goes wrong, then give up.
pub async fn extract_continuously(
    tx: mpsc::Sender<Record>,
    stop: CancellationToken,
    rpc_url: String,
    since_block: Option<u64>,
) {
    let mut since_block = since_block;
    let mut retries = 0;
    loop {
        match do_extract_continuously(&tx, stop.clone(), &rpc_url, &mut since_block).await {
            Ok(()) => break,
            Err(e) => {
                tracing::error!("Failed to extract: {e:?}");
                retries += 1;
                if retries > 3 {
                    tracing::error!("Giving up after 3 retries.");
                    break;
                }
            }
        }
    }
}
