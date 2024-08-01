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

    /// If set, do not talk to the network and do not fill the database,
    /// but only serve the web interface with the already existing data
    #[clap(short = 'N', long)]
    pub dry: bool,

    /// The address of a Solana RPC node
    #[clap(short, long, default_value = "https://api.mainnet-beta.solana.com")]
    pub url: String,

    /// The directory to store the database in
    #[clap(short = 'Z', long, default_value = ".store")]
    pub store_path: String,
}
