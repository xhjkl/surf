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

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PrettyVote {
    pub signature: String,
    pub block: u64,
    pub timestamp: u64,
    pub author: String,
    pub target: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PrettyTransfer {
    pub signature: String,
    pub block: u64,
    pub timestamp: u64,
    pub source: String,
    pub destination: String,
    pub lamports: u64,
}

impl From<Vote> for PrettyVote {
    fn from(vote: Vote) -> Self {
        Self {
            signature: vote.signature.to_string(),
            block: vote.block_index,
            timestamp: vote.timestamp,
            author: vote.author.to_string(),
            target: vote.target.to_string(),
        }
    }
}

impl From<Transfer> for PrettyTransfer {
    fn from(transfer: Transfer) -> Self {
        Self {
            signature: transfer.signature.to_string(),
            block: transfer.block_index,
            timestamp: transfer.timestamp,
            source: transfer.source.to_string(),
            destination: transfer.destination.to_string(),
            lamports: transfer.lamports,
        }
    }
}
