/* spellchecker:words blockheight */
//! What the users see.

use std::fmt::Debug;
use std::sync::Arc;

use actix_web::{web, App, HttpServer};
use std::net::ToSocketAddrs;
use tokio_util::sync::CancellationToken;

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
    let all_votes = store.get_all_votes().await?;

    let mut votes = Vec::with_capacity(all_votes.len());
    for vote in all_votes {
        if let Some(ref signature) = filters.signature {
            if vote.signature.to_string() != *signature {
                continue;
            }
        }
        if let Some(block) = filters.block {
            if vote.block_index != block {
                continue;
            }
        }
        if let Some(ref to) = filters.to {
            if vote.target.to_string() != *to {
                continue;
            }
        }
        if let Some(ref from) = filters.from {
            if vote.author.to_string() != *from {
                continue;
            }
        }
        votes.push(vote);
    }

    Ok(serde_json::to_string(&votes)?)
}

async fn get_transfers(
    store: web::Data<Arc<Store>>,
    web::Query(filters): web::Query<Criteria>,
) -> Result<String> {
    let all_transfers = store.get_all_transfers().await?;

    let mut transfers = Vec::with_capacity(all_transfers.len());
    for transfer in all_transfers {
        if let Some(ref signature) = filters.signature {
            if transfer.signature.to_string() != *signature {
                continue;
            }
        }
        if let Some(block) = filters.block {
            if transfer.block_index != block {
                continue;
            }
        }
        if let Some(ref to) = filters.to {
            if transfer.destination.to_string() != *to {
                continue;
            }
        }
        if let Some(ref from) = filters.from {
            if transfer.source.to_string() != *from {
                continue;
            }
        }
        transfers.push(transfer);
    }

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
