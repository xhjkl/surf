//! An application-specific result type.

use actix_web::http::StatusCode;
use actix_web::ResponseError;
use thiserror::Error;

/// A custom error type for our application.
#[derive(Error, Debug)]
pub enum Error {
    #[error("while calling into the underlying OS: {0}")]
    ExpectationViolation(#[from] std::io::Error),
    #[error("not found")]
    NotFound,
    #[error("failed to open the database: {0}")]
    Database(#[from] rocksdb::Error),
    #[error("failed to deserialize: {0}")]
    Coding(#[from] postcard::Error),
    #[error("failed to serialize: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("failed to communicate with the cluster: {0}")]
    SolanaClient(#[from] solana_client::client_error::ClientError),
    #[error("bad signature: {0}")]
    SolanaBadSignature(#[from] solana_sdk::signature::ParseSignatureError),
    #[error("bad account address: {0}")]
    SolanaBadPubkey(#[from] solana_sdk::pubkey::ParsePubkeyError),
    #[error("bad numeric: {0}")]
    SolanaBadNumber(String),
}

/// A specialization of `std::result::Result` for our application.
/// The `Error` type is a custom error type.
pub type Result<T> = std::result::Result<T, Error>;

impl ResponseError for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::NotFound => StatusCode::NOT_FOUND,
            Error::SolanaBadSignature(_) => StatusCode::BAD_REQUEST,
            Error::SolanaBadPubkey(_) => StatusCode::BAD_REQUEST,
            Error::SolanaBadNumber(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
