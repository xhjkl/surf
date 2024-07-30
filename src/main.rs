#![doc = include_str!("../README.md")]

use std::sync::Arc;

use clap::Parser;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt as _, EnvFilter};

mod args;
use args::Args;

mod record;

mod result;
use result::Result;

mod store;
use store::{store_all_records_from, Store};

mod extraction;
use extraction::extract_continuously;

mod interface;
use interface::serve_forever;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    tracing::info!("Starting...");

    let store = Arc::new(Store::with_path(".store").await?);

    let stop = CancellationToken::new();

    let (tx, rx) = mpsc::channel(1);

    let last_known_block = store.last_known_block().await;
    tracing::trace!("Last known block index: {:?}", last_known_block);

    let extractor = tokio::spawn(extract_continuously(
        tx,
        stop.clone(),
        args.url.to_owned(),
        last_known_block,
    ));

    let committer = tokio::spawn(store_all_records_from(rx, store.clone(), stop.clone()));

    serve_forever((args.host, args.port), store.clone(), stop.clone()).await?;

    // Assuming `actix-web` has already handled the SIGINT:
    stop.cancel();
    tracing::info!("Received SIGINT; waiting for the network to finish...");

    for task in [extractor, committer].into_iter() {
        let awaited = task.await;
        if let Err(e) = awaited {
            tracing::error!("Failed to rejoin a background task: {e:?}");
        }
    }

    tracing::info!("Stopped");

    Ok(())
}
