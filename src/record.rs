//! Structures reused across the modules.

use solana_sdk::{pubkey::Pubkey, signature::Signature};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Vote {
    pub signature: Signature,
    pub block_index: u64,
    pub timestamp: u64,
    pub author: Pubkey,
    pub target: Pubkey,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Transfer {
    pub signature: Signature,
    pub block_index: u64,
    pub timestamp: u64,
    pub source: Pubkey,
    pub destination: Pubkey,
    pub lamports: u64,
}

/// What is gotten from the network and passed to the database.
#[derive(Clone, Debug)]
pub enum Record {
    Vote(Vote),
    Transfer(Transfer),
}
