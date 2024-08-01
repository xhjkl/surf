/* spellchecker:words blockheight */
//! What the users see.

use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use solana_sdk::pubkey::Pubkey;
use std::net::ToSocketAddrs;
use tokio_util::sync::CancellationToken;

mod finding_transfers;
mod finding_votes;

use crate::record::{PrettyTransfer, PrettyVote};
use crate::store::Store;
use crate::Result;

/// What a user can filter by using the query string.
#[derive(Debug, serde::Deserialize)]
struct Criteria {
    block: Option<u64>,
    signature: Option<String>,
    to: Option<String>,
    from: Option<String>,
}

async fn index() -> &'static str {
    "Refer to README.md for more information."
}

async fn get_last_known_block(store: web::Data<Arc<Store>>) -> Result<String> {
    let last_known_block = store.last_known_block().await;
    Ok(last_known_block.map_or_else(|| "null".to_owned(), |block| block.to_string()))
}

async fn get_votes(
    store: web::Data<Arc<Store>>,
    web::Query(filters): web::Query<Criteria>,
) -> Result<String> {
    use finding_votes::{
        find_votes_with_block_index, find_votes_with_full_scan, find_votes_with_signature,
    };

    let store = store.get_ref();
    let votes = match (
        &filters.signature,
        &filters.block,
        &filters.to,
        &filters.from,
    ) {
        (Some(signature), None, None, None) => find_votes_with_signature(store, signature).await,
        (None, Some(block), None, None) => find_votes_with_block_index(store, *block).await,
        _ => {
            let block_index = filters.block;
            let to = filters.to.as_deref().map(Pubkey::from_str).transpose()?;
            let from = filters.from.as_deref().map(Pubkey::from_str).transpose()?;
            find_votes_with_full_scan(store, block_index, to, from).await
        }
    };
    let votes = votes?.into_iter().map(PrettyVote::from).collect::<Vec<_>>();
    Ok(serde_json::to_string(&votes)?)
}

async fn get_transfers(
    store: web::Data<Arc<Store>>,
    web::Query(filters): web::Query<Criteria>,
) -> Result<String> {
    use finding_transfers::{
        find_transfers_with_block_index, find_transfers_with_full_scan,
        find_transfers_with_signature,
    };

    let store = store.get_ref();
    let transfers = match (
        &filters.signature,
        &filters.block,
        &filters.to,
        &filters.from,
    ) {
        (Some(signature), None, None, None) => {
            find_transfers_with_signature(store, signature).await
        }
        (None, Some(block), None, None) => find_transfers_with_block_index(store, *block).await,
        _ => {
            let block_index = filters.block;
            let to = filters.to.as_deref().map(Pubkey::from_str).transpose()?;
            let from = filters.from.as_deref().map(Pubkey::from_str).transpose()?;
            find_transfers_with_full_scan(store, block_index, to, from).await
        }
    };
    let transfers = transfers?
        .into_iter()
        .map(PrettyTransfer::from)
        .collect::<Vec<_>>();
    Ok(serde_json::to_string(&transfers)?)
}

/// Run the server.
pub async fn serve_forever<Address>(
    address: Address,
    store: Arc<Store>,
    _stop: CancellationToken,
) -> Result<()>
where
    Address: ToSocketAddrs + Debug,
{
    tracing::info!("Starting web server on {address:?}...");
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(store.clone()))
            .route("/", web::get().to(index))
            .route("/blockheight", web::get().to(get_last_known_block))
            .route("/votes", web::get().to(get_votes))
            .route("/transfers", web::get().to(get_transfers))
    })
    .bind(address)?
    .run()
    .await?;

    Ok(())
}
