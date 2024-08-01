//! Bridge between the db and the web interface.

use std::str::FromStr;
use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

use crate::record::Vote;
use crate::result::Error;
use crate::store::Store;
use crate::Result;

pub async fn find_votes_with_block_index(
    store: &Arc<Store>,
    block_index: u64,
) -> Result<Vec<Vote>> {
    store
        .find_votes_by_block_index(block_index)
        .await
        .map(|results| {
            results
                .into_iter()
                .filter(|x| x.block_index == block_index)
                .collect()
        })
}

pub async fn find_votes_with_signature(store: &Arc<Store>, signature: &str) -> Result<Vec<Vote>> {
    let signature = Signature::from_str(signature)?;

    let Some(vote) = store.find_vote(&signature).await else {
        return Err(Error::NotFound);
    };
    Ok(vec![vote])
}

pub async fn find_votes_with_full_scan(
    store: &Arc<Store>,
    block: Option<u64>,
    to: Option<Pubkey>,
    from: Option<Pubkey>,
) -> Result<Vec<Vote>> {
    let all_votes = store.find_all_votes().await?;

    let mut votes = Vec::with_capacity(all_votes.len());
    for vote in all_votes {
        if let Some(block) = block {
            if vote.block_index != block {
                continue;
            }
        }
        if let Some(ref to) = to {
            if vote.target != *to {
                continue;
            }
        }
        if let Some(ref from) = from {
            if vote.author != *from {
                continue;
            }
        }
        votes.push(vote);
    }
    Ok(votes)
}
