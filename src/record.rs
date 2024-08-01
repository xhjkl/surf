//! Structures reused across the modules.

use solana_sdk::{pubkey::Pubkey, signature::Signature};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub struct Vote {
    pub signature: Signature,
    pub block_index: u64,
    pub timestamp: u64,
    pub author: Pubkey,
    pub target: Pubkey,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
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

// Forcing the keys to be pretty.
impl serde::Serialize for Vote {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("signature", &self.signature.to_string())?;
        map.serialize_entry("block", &self.block_index)?;
        map.serialize_entry("timestamp", &self.timestamp)?;
        map.serialize_entry("author", &self.author.to_string())?;
        map.serialize_entry("target", &self.target.to_string())?;
        map.end()
    }
}

impl serde::Serialize for Transfer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("signature", &self.signature.to_string())?;
        map.serialize_entry("block", &self.block_index)?;
        map.serialize_entry("timestamp", &self.timestamp)?;
        map.serialize_entry("source", &self.source.to_string())?;
        map.serialize_entry("destination", &self.destination.to_string())?;
        map.serialize_entry("lamports", &self.lamports)?;
        map.end()
    }
}
