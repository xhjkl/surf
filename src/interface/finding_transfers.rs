//! Bridge between the db and the web interface.

use std::str::FromStr;
use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

use crate::record::Transfer;
use crate::result::Error;
use crate::store::Store;
use crate::Result;

pub async fn find_transfers_with_block_index(
    store: &Arc<Store>,
    block_index: u64,
) -> Result<Vec<Transfer>> {
    store
        .find_transfers_by_block_index(block_index)
        .await
        .map(|results| {
            results
                .into_iter()
                .filter(|x| x.block_index == block_index)
                .collect()
        })
}

pub async fn find_transfers_with_signature(
    store: &Arc<Store>,
    signature: &str,
) -> Result<Vec<Transfer>> {
    let signature = Signature::from_str(signature)?;

    let Some(transfer) = store.find_transfer(&signature).await else {
        return Err(Error::NotFound);
    };
    Ok(vec![transfer])
}

pub async fn find_transfers_with_full_scan(
    store: &Arc<Store>,
    block: Option<u64>,
    to: Option<Pubkey>,
    from: Option<Pubkey>,
) -> Result<Vec<Transfer>> {
    let all_transfers = store.find_all_transfers().await?;

    let mut transfers = Vec::with_capacity(all_transfers.len());
    for transfer in all_transfers {
        if let Some(block) = block {
            if transfer.block_index != block {
                continue;
            }
        }
        if let Some(ref to) = to {
            if transfer.destination != *to {
                continue;
            }
        }
        if let Some(ref from) = from {
            if transfer.source != *from {
                continue;
            }
        }
        transfers.push(transfer);
    }
    Ok(transfers)
}
