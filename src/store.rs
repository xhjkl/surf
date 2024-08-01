/* spellchecker:words rocksdb stdvec */
//! Everything we remember.

use serde::Serialize;
use solana_sdk::signature::Signature;
use std::sync::Arc;
use tokio::{select, sync::mpsc::Receiver};
use tokio_util::sync::CancellationToken;

use crate::record::{Record, Transfer, Vote};
use crate::Result;

/// A database of records.
pub struct Store {
    db: rocksdb::DB,
}

const VOTES_NS: &str = "vote";
const TRANSFERS_NS: &str = "transfer";
const VOTES_INDEX_NS: &str = "+votes";
const TRANSFERS_INDEX_NS: &str = "+transfers";
impl Store {
    async fn make_new_with_path<Path: AsRef<std::path::Path>>(path: Path) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);

        // RocksDB's `create_if_missing` does not create any column families.
        // And, when opening an existing database, we need to supply all the existing ones.
        let mut db = rocksdb::DB::open(&opts, path)?;
        db.create_cf(VOTES_NS, &rocksdb::Options::default())?;
        db.create_cf(TRANSFERS_NS, &rocksdb::Options::default())?;
        db.create_cf(VOTES_INDEX_NS, &rocksdb::Options::default())?;
        db.create_cf(TRANSFERS_INDEX_NS, &rocksdb::Options::default())?;
        Ok(Self { db })
    }

    async fn open_existing_with_path<Path: AsRef<std::path::Path>>(path: Path) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(false);

        let db = rocksdb::DB::open_cf(
            &opts,
            path,
            vec![VOTES_NS, TRANSFERS_NS, VOTES_INDEX_NS, TRANSFERS_INDEX_NS],
        )?;
        Ok(Self { db })
    }

    /// Open a store at the given path, creating it if necessary.
    pub async fn with_path<Path: AsRef<std::path::Path>>(path: Path) -> Result<Self> {
        let db = Self::open_existing_with_path(&path).await;
        if let Ok(db) = db {
            return Ok(db);
        }
        let db = Self::make_new_with_path(&path).await?;
        Ok(db)
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
    fn associate<T, Y>(
        &self,
        cf: &rocksdb::ColumnFamily,
        secondary_key: &T,
        primary_key: &Y,
    ) -> Result<()>
    where
        T: Sized + Serialize,
        Y: Sized + Serialize,
    {
        let bytes = Vec::with_capacity(64);
        let bytes = postcard::to_extend(&secondary_key, bytes)?;
        let bytes = postcard::to_extend(&primary_key, bytes)?;

        let primary_key = postcard::to_stdvec(&primary_key).unwrap();

        self.db.put_cf(cf, bytes, primary_key)?;
        Ok(())
    }
}

impl Store {
    /// Update the last known block to the given value
    /// if it is greater than the current one.
    async fn bump_last_known_block(&self, block_index: u64) -> Result<()> {
        let last_known_block = self.last_known_block().await.unwrap_or(0);
        if block_index > last_known_block {
            self.set_last_known_block(block_index).await?;
        }
        Ok(())
    }

    /// Write down a Vote record, possibly overwriting the same primary-keyed record.
    pub async fn save_vote(&self, vote: &Vote) -> Result<()> {
        self.bump_last_known_block(vote.block_index).await?;

        // Writing down the contents:
        let cf = self.db.cf_handle(VOTES_NS).unwrap();
        let key = postcard::to_stdvec(&vote.signature).unwrap();
        self.db.put_cf(cf, key, postcard::to_stdvec(&vote)?)?;

        // Indexing:
        let cf = self.db.cf_handle(VOTES_INDEX_NS).unwrap();
        self.associate(cf, &vote.block_index, &vote.signature)?;
        self.associate(cf, &vote.target, &vote.signature)?;
        self.associate(cf, &vote.author, &vote.signature)?;

        Ok(())
    }

    /// Write down a Transfer record, possibly overwriting the same primary-keyed record.
    pub async fn save_transfer(&self, transfer: &Transfer) -> Result<()> {
        self.bump_last_known_block(transfer.block_index).await?;

        // The contents:
        let cf = self.db.cf_handle(TRANSFERS_NS).unwrap();
        let key = postcard::to_stdvec(&transfer.signature).unwrap();
        self.db.put_cf(cf, key, postcard::to_stdvec(&transfer)?)?;

        // Indexing:
        let cf = self.db.cf_handle(TRANSFERS_INDEX_NS).unwrap();
        self.associate(cf, &transfer.block_index, &transfer.signature)?;
        self.associate(cf, &transfer.source, &transfer.signature)?;
        self.associate(cf, &transfer.destination, &transfer.signature)?;
        self.associate(cf, &transfer.lamports, &transfer.signature)?;

        Ok(())
    }
}

impl Store {
    /// Get the unique Vote record with the given primary key if it exists.
    pub async fn find_vote(&self, key: &Signature) -> Option<Vote> {
        let cf = self.db.cf_handle(VOTES_NS).unwrap();
        let key = postcard::to_stdvec(&key).unwrap();
        let vote = self.db.get_pinned_cf(cf, key).ok().flatten()?;

        let Ok(vote) = postcard::from_bytes(&vote) else {
            return None;
        };
        Some(vote)
    }

    /// Retrieve the unique Transfer record with the given primary key if it exists.
    pub async fn find_transfer(&self, key: &Signature) -> Option<Transfer> {
        let cf = self.db.cf_handle(TRANSFERS_NS).unwrap();
        let key = postcard::to_stdvec(&key).unwrap();
        let transfer = self.db.get_pinned_cf(cf, key).ok().flatten()?;

        let Ok(transfer) = postcard::from_bytes(&transfer) else {
            return None;
        };
        Some(transfer)
    }

    /// Retrieve all the matching records from the database.
    pub async fn find_all_votes(&self) -> Result<Vec<Vote>> {
        let mut votes = Vec::new();
        for each in self.db.full_iterator_cf(
            self.db.cf_handle(VOTES_NS).unwrap(),
            rocksdb::IteratorMode::Start,
        ) {
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

    /// Retrieve all the matching records from the database.
    pub async fn find_all_transfers(&self) -> Result<Vec<Transfer>> {
        let mut transfers = Vec::new();
        for each in self.db.full_iterator_cf(
            self.db.cf_handle(TRANSFERS_NS).unwrap(),
            rocksdb::IteratorMode::Start,
        ) {
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

    /// Retrieve all the matching records from the database.
    pub async fn find_votes_by_block_index(&self, block_index: u64) -> Result<Vec<Vote>> {
        let cf = self.db.cf_handle(VOTES_INDEX_NS).unwrap();
        let prefix = postcard::to_stdvec(&block_index).unwrap();

        let mut votes = Vec::new();
        for each in self.db.prefix_iterator_cf(cf, prefix) {
            let Ok((_k, v)) = each else {
                tracing::error!("Failed to get a row from the database");
                continue;
            };
            let Ok(key) = postcard::from_bytes::<Signature>(&v) else {
                continue;
            };
            let Some(vote) = self.find_vote(&key).await else {
                tracing::error!("Dangling index entry for a vote");
                continue;
            };
            votes.push(vote);
        }
        Ok(votes)
    }

    /// Retrieve all the matching records from the database.
    pub async fn find_transfers_by_block_index(&self, block_index: u64) -> Result<Vec<Transfer>> {
        let cf = self.db.cf_handle(TRANSFERS_INDEX_NS).unwrap();
        let prefix = postcard::to_stdvec(&block_index).unwrap();

        let mut transfers = Vec::new();
        for each in self.db.prefix_iterator_cf(cf, prefix) {
            let Ok((_k, v)) = each else {
                tracing::error!("Failed to get a row from the database");
                continue;
            };
            let Ok(key) = postcard::from_bytes::<Signature>(&v) else {
                continue;
            };
            let Some(transfer) = self.find_transfer(&key).await else {
                tracing::error!("Dangling index entry for a transfer");
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
        match record {
            Record::Vote(vote) => {
                let res = store.save_vote(&vote).await;
                if let Err(e) = res {
                    tracing::error!("Failed to store a vote: {e:?}");
                    return;
                }
            }
            Record::Transfer(transfer) => {
                let res = store.save_transfer(&transfer).await;
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

    impl Store {
        fn disposable_path() -> std::path::PathBuf {
            use rand::Rng;

            let mut rng = rand::thread_rng();
            let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            path.push("...store");
            path.push(rng.gen::<u64>().to_string());
            path
        }

        async fn disposable() -> Result<Self> {
            Self::with_path(&Self::disposable_path()).await
        }
    }

    #[tokio::test]
    async fn last_known_block_persists() {
        // Given an empty store:
        let store = Store::disposable().await.unwrap();

        // When we query the last known block from it:
        let last_known_block = store.last_known_block().await;

        // Then it should be unset:
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
    async fn votes_found_by_key() {
        // Given a store with some data:
        let signature = Signature::new_unique();
        let vote = Vote {
            signature,
            block_index: 777,
            timestamp: 1234567890,
            author: Pubkey::new_unique(),
            target: Pubkey::new_unique(),
        };
        let store = Store::disposable().await.unwrap();
        store.save_vote(&vote).await.unwrap();

        // When we query a datum by its primary key:
        let gotten = store.find_vote(&signature).await;

        // Then it should be the same:
        assert_eq!(gotten, Some(vote.clone()));

        // And when we query a datum by its secondary key:
        let gotten = store.find_all_votes().await.unwrap();

        // Then it should be the same:
        assert!(gotten.contains(&vote));
    }

    #[tokio::test]
    async fn votes_found_in_everything() {
        // Given a store with some data:
        let signature = Signature::new_unique();
        let vote = Vote {
            signature,
            block_index: 777,
            timestamp: 1234567890,
            author: Pubkey::new_unique(),
            target: Pubkey::new_unique(),
        };
        let store = Store::disposable().await.unwrap();
        store.save_vote(&vote).await.unwrap();

        // When we query all data:
        let gotten = store.find_all_votes().await.unwrap();

        // Then it should have the original datum:
        assert!(gotten.contains(&vote));
    }

    #[tokio::test]
    async fn transfers_found_by_key() {
        // Given a store with some data:
        let signature = Signature::new_unique();
        let transfer = Transfer {
            signature,
            block_index: 777,
            timestamp: 1234567890,
            source: Pubkey::new_unique(),
            destination: Pubkey::new_unique(),
            lamports: 0,
        };
        let store = Store::disposable().await.unwrap();
        store.save_transfer(&transfer).await.unwrap();

        // When we query a datum by its primary key:
        let gotten = store.find_transfer(&signature).await;

        // Then it should be the same:
        assert_eq!(gotten, Some(transfer.clone()));
    }

    #[tokio::test]
    async fn transfers_found_in_everything() {
        // Given a store with some data:
        let signature = Signature::new_unique();
        let transfer = Transfer {
            signature,
            block_index: 777,
            timestamp: 1234567890,
            source: Pubkey::new_unique(),
            destination: Pubkey::new_unique(),
            lamports: 0,
        };
        let store = Store::disposable().await.unwrap();
        store.save_transfer(&transfer).await.unwrap();

        // When we query all data:
        let gotten = store.find_all_transfers().await.unwrap();

        // Then it should have the original datum:
        assert!(gotten.contains(&transfer));
    }

    #[tokio::test]
    async fn votes_and_transfers_are_isolated() {
        // Given a store with some data:
        let signature = Signature::new_unique();
        let vote = Vote {
            signature,
            block_index: 777,
            timestamp: 1234567890,
            author: Pubkey::new_unique(),
            target: Pubkey::new_unique(),
        };
        let transfer = Transfer {
            signature,
            block_index: 777,
            timestamp: 1234567890,
            source: Pubkey::new_unique(),
            destination: Pubkey::new_unique(),
            lamports: 0,
        };
        let store = Store::disposable().await.unwrap();
        store.save_vote(&vote).await.unwrap();
        store.save_transfer(&transfer).await.unwrap();

        // When we query all data:
        let gotten = store.find_all_votes().await.unwrap();

        // Then it should have the original datum:
        assert!(gotten.contains(&vote));
        // ... and not have anything else:
        assert!(gotten.len() == 1);

        // And when we query all data:
        let gotten = store.find_all_transfers().await.unwrap();

        // Then it should have the original datum:
        assert!(gotten.contains(&transfer));
        // ... and not have anything else:
        assert!(gotten.len() == 1);
    }

    #[tokio::test]
    async fn votes_found_by_index() {
        // Given a store with some data having the same block index:
        let vote = Vote {
            signature: Signature::new_unique(),
            block_index: 777,
            timestamp: 1234567890,
            author: Pubkey::new_unique(),
            target: Pubkey::new_unique(),
        };
        let vote2 = Vote {
            signature: Signature::new_unique(),
            block_index: 777,
            timestamp: 1234567891,
            author: Pubkey::new_unique(),
            target: Pubkey::new_unique(),
        };
        let store = Store::disposable().await.unwrap();
        store.save_vote(&vote).await.unwrap();
        store.save_vote(&vote2).await.unwrap();

        // When we query by that common block index:
        let gotten = store.find_votes_by_block_index(777).await.unwrap();

        // Then it should be found:
        assert!(gotten.contains(&vote));
        assert!(gotten.contains(&vote2));
        assert_eq!(gotten.len(), 2);
    }

    #[tokio::test]
    async fn transfers_found_by_index() {
        // Given a store with some data having the same block index:
        let transfer = Transfer {
            signature: Signature::new_unique(),
            block_index: 777,
            timestamp: 1234567890,
            source: Pubkey::new_unique(),
            destination: Pubkey::new_unique(),
            lamports: 0,
        };
        let transfer2 = Transfer {
            signature: Signature::new_unique(),
            block_index: 777,
            timestamp: 1234567891,
            source: Pubkey::new_unique(),
            destination: Pubkey::new_unique(),
            lamports: 0,
        };
        let store = Store::disposable().await.unwrap();
        store.save_transfer(&transfer).await.unwrap();
        store.save_transfer(&transfer2).await.unwrap();

        // When we query by that common block index:
        let gotten = store.find_transfers_by_block_index(777).await.unwrap();

        // Then it should be found:
        assert!(gotten.contains(&transfer));
        assert!(gotten.contains(&transfer2));
        assert_eq!(gotten.len(), 2);
    }
}
