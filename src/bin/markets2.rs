use std::time::Duration;

use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use futures_util::{TryFutureExt, future::join_all};
use prediction_data_ingestor::*;
use reqwest::Client;
use tokio::time::sleep;

fn encode_number_to_base64(n: u64) -> String {
    let num_str = n.to_string(); // e.g., "1500"
    general_purpose::STANDARD.encode(num_str.as_bytes())
}

#[tokio::main]
async fn main() -> Result<()> {
    let data = fetch_markets().await?;
    println!("got all markets with length: {}", data.len());
    Ok(())
}

async fn fetch_markets() -> Result<Vec<PolymarketMarket>> {
    let mut n_pages = 100;
    let mut ids = (0..n_pages).collect::<Vec<_>>();
    let client = Client::new();
    let mut data = Vec::new();

    while !ids.is_empty() {
        const MAX_CONCURRENCY: usize = 100;
        let handles = ids
            .drain(0..MAX_CONCURRENCY.min(ids.len()))
            .map(|id| {
                get_page(
                    client.clone(),
                    if id == 0 {
                        None
                    } else {
                        Some(encode_number_to_base64(id * 500))
                    },
                )
                .map_err(move |e| (id, e))
                .map_ok(move |result| (id, result))
            })
            .collect::<Vec<_>>();

        let results = join_all(handles).await;
        for result in results {
            match result {
                Ok((page, result)) => {
                    println!("{}: len={:?}", page, result.data.len());
                    if page == n_pages - 1 {
                        if result.data.len() == 0 {
                            println!("{} was empty; no more pages", page);
                        } else {
                            println!("adding {} more pages to query", MAX_CONCURRENCY);
                            ids.extend(n_pages..(n_pages+MAX_CONCURRENCY as u64));
                            n_pages += MAX_CONCURRENCY as u64;
                        }
                    }

                    data.extend(result.data);
                }
                Err((page, e)) => {
                    ids.push(page);
                    println!("{}: error={:?}", page, e);
                }
            }
        }

        if !ids.is_empty() {
            sleep(Duration::from_secs(5)).await;
        }
    }

    Ok(data)
}

async fn get_page(client: Client, cursor: Option<String>) -> Result<ApiResponse> {
    let mut url = "https://clob.polymarket.com/markets".to_string();
    if let Some(c) = &cursor {
        url.push_str(&format!("?next_cursor={}", c));
    }

    let res = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let de = &mut serde_json::Deserializer::from_slice(&res);
    let result: ApiResponse = serde_path_to_error::deserialize(de)?;
    Ok(result)
}
