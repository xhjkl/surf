use clap::{self, Parser};

/// A small indexer.
#[derive(Parser, Debug)]
#[clap()]
pub struct Args {
    /// The port to listen on for the web interface
    #[clap(short = 'P', long, default_value_t = 8989)]
    pub port: u16,

    /// The host to listen on for the web interface
    #[clap(short = 'H', long, default_value = "localhost")]
    pub host: String,

    /// The address of a Solana RPC node
    #[clap(short, long, default_value = "https://api.mainnet-beta.solana.com")]
    pub url: String,

    /// The directory to store the database in
    #[clap(short = 'Z', long, default_value = ".store")]
    pub store: String,
}
