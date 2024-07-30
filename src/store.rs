/* spellchecker:words rocksdb stdvec */
//! Everything we remember.

use std::sync::Arc;
use serde::Serialize;
use tokio::{select, sync::mpsc::Receiver};
use tokio_util::sync::CancellationToken;

use crate::record::{Record, Transfer, Vote};
use crate::Result;

/// A database of records.
pub struct Store {
    db: rocksdb::DB,
}

impl Store {
    /// Open a store at the given path, creating it if necessary.
    pub async fn with_path<Path: AsRef<std::path::Path>>(dir: Path) -> Result<Self> {
        let db = rocksdb::DB::open_default(dir)?;
        Ok(Self { db })
    }
}

const LAST_KNOWN_BLOCK_KEY: &[u8] = b"\x1b\x11";
impl Store {
    /// Maximum of all the "block index" fields across all the records.
    pub async fn last_known_block(&self) -> Option<u64> {
        let gotten = self.db.get_pinned(LAST_KNOWN_BLOCK_KEY).ok().flatten()?;
        postcard::from_bytes(&gotten).ok()
    }

    /// Set the last known block to the given value.
    pub async fn set_last_known_block(&self, block: u64) -> Result<()> {
        let bytes = postcard::to_stdvec(&block).unwrap();
        self.db.put(LAST_KNOWN_BLOCK_KEY, bytes)?;
        Ok(())
    }
}

impl Store {
    /// Add a record of `{secondary_key}:{primary_key} -> {primary_key}` to the database
    /// so that it could later be retrieved by a prefix scan.
    fn associate<T, Y>(&self, secondary_key: &T, primary_key: &Y) -> Result<()>
    where
        T: Sized + Serialize,
        Y: Sized + Serialize,
    {
        let bytes = Vec::with_capacity(64);
        let bytes = postcard::to_extend(&secondary_key, bytes)?;
        let bytes = postcard::to_extend(&primary_key, bytes)?;

        let primary_key = postcard::to_stdvec(&primary_key).unwrap();

        self.db.put(bytes, primary_key)?;
        Ok(())
    }
}

impl Store {
    async fn bump_last_known_block(&self, block_index: u64) -> Result<()> {
        let last_known_block = self.last_known_block().await.unwrap_or(0);
        if block_index > last_known_block {
            self.set_last_known_block(block_index).await?;
        }
        Ok(())
    }

    /// Write down a Vote record, possibly overwriting the same primary-keyed record.
    pub async fn add_vote(&self, vote: &Vote) -> Result<()> {
        self.bump_last_known_block(vote.block_index).await?;

        // Writing down the contents:
        let key = postcard::to_stdvec(&vote.signature).unwrap();
        self.db.put(key, postcard::to_stdvec(&vote)?)?;

        // Indexing:
        self.associate(&vote.target, &vote.signature)?;
        self.associate(&vote.author, &vote.signature)?;

        Ok(())
    }

    /// Write down a Transfer record, possibly overwriting the same primary-keyed record.
    pub async fn add_transfer(&self, transfer: &Transfer) -> Result<()> {
        self.bump_last_known_block(transfer.block_index).await?;

        // The contents:
        let key = postcard::to_stdvec(&transfer.signature).unwrap();
        self.db.put(key, postcard::to_stdvec(&transfer)?)?;

        // Indexing:
        self.associate(&transfer.source, &transfer.signature)?;
        self.associate(&transfer.destination, &transfer.signature)?;
        self.associate(&transfer.lamports, &transfer.signature)?;

        Ok(())
    }
}

impl Store {
    /// Get all the matching records from the database.
    pub async fn get_all_votes(&self) -> Result<Vec<Vote>> {
        let mut votes = Vec::new();
        for each in self.db.full_iterator(rocksdb::IteratorMode::Start) {
            let Ok((_k, v)) = each else {
                tracing::error!("Failed to get a row from the database");
                continue;
            };
            let Ok(vote) = postcard::from_bytes(&v) else {
                continue;
            };
            votes.push(vote);
        }
        Ok(votes)
    }

    /// Get all the matching records from the database.
    pub async fn get_all_transfers(&self) -> Result<Vec<Transfer>> {
        let mut transfers = Vec::new();
        for each in self.db.full_iterator(rocksdb::IteratorMode::Start) {
            let Ok((_k, v)) = each else {
                tracing::error!("Failed to get a row from the database");
                continue;
            };
            let Ok(transfer) = postcard::from_bytes(&v) else {
                continue;
            };
            transfers.push(transfer);
        }
        Ok(transfers)
    }
}

/// [store_all_records_from] sans cancellation.
async fn do_store_all_records_from(mut rx: Receiver<Record>, store: Arc<Store>) {
    while let Some(record) = rx.recv().await {
        tracing::trace!("Got record: {record:?}");
        match record {
            Record::Vote(vote) => {
                let res = store.add_vote(&vote).await;
                if let Err(e) = res {
                    tracing::error!("Failed to store a vote: {e:?}");
                    return;
                }
            }
            Record::Transfer(transfer) => {
                let res = store.add_transfer(&transfer).await;
                if let Err(e) = res {
                    tracing::error!("Failed to store a transfer: {e:?}");
                    return;
                }
            }
        }
    }
}

/// Drain the channel and commit the records to the database.
pub async fn store_all_records_from(
    rx: Receiver<Record>,
    store: Arc<Store>,
    stop: CancellationToken,
) {
    select! {
        biased; // Making sure the signal gets polled first.
        _ = stop.cancelled() => {
            tracing::trace!("Storing cancelled");
        }
        _ = do_store_all_records_from(rx, store) => {
            tracing::trace!("Stream depleted");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use solana_sdk::pubkey::Pubkey;

    #[tokio::test]
    #[ignore = "run manually with a clean store"]
    async fn last_known_block_persists() {
        // Given an empty store:
        let store = Store::with_path("...store").await.unwrap();

        // When we query the last known block from it:
        let last_known_block = store.last_known_block().await;

        // Then it should be 0:
        assert!(last_known_block.is_none());

        // And when we set the last known block to a certain value:
        let lucky_eight = 8888;
        store.set_last_known_block(lucky_eight).await.unwrap();

        // And when we query it again:
        let last_known_block = store.last_known_block().await;

        // Then it should be the same:
        assert_eq!(last_known_block, Some(lucky_eight));
    }

    #[tokio::test]
    async fn it_works() {
        // Given a store with some data:
        let signature = Signature::new_unique();
        let vote = Vote {
            signature,
            block_index: 777,
            timestamp: 1234567890,
            author: Pubkey::new_unique(),
            target: Pubkey::new_unique(),
        };
        let store = Store::with_path("...store").await.unwrap();
        store.add_vote(&vote).await.unwrap();

        // When we query a datum by its primary key:
        let gotten = store.get_vote(&signature).await;

        // Then it should be the same:
        assert_eq!(gotten, Some(vote.clone()));

        // And when we query a datum by its secondary key:
        let gotten = store.get_all_votes().await.unwrap();

        // Then it should be the same:
        assert!(gotten.contains(&vote));
    }
}
