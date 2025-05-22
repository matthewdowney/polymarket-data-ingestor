use std::collections::HashMap;

use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ApiResponse {
    pub data: Vec<Market>,
    pub next_cursor: Option<String>,
    pub limit: u32,
    pub count: u32,
}

#[derive(Debug, Deserialize)]
pub struct Market {
    // unclear what the differences between these fields are
    pub closed: bool,
    pub accepting_orders: bool,
    pub active: bool,
    pub archived: bool,

    pub condition_id: String,
    pub question_id: String,
    pub question: String,
    pub description: String,

    // there are inconsistencies in the other fields, so treat them dynamically
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new();
    let mut cursor = None;

    let mut page = 0;
    loop {
        page += 1;
        let mut url = "https://clob.polymarket.com/markets".to_string();
        if let Some(c) = &cursor {
            url.push_str(&format!("?next_cursor={}", c));
        }
        println!("{}", url);

        let res = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let de = &mut serde_json::Deserializer::from_slice(&res);
        let result: ApiResponse = match serde_path_to_error::deserialize(de) {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Deserialization error at {}: {}", e.path(), e);
                return Err(e.into());
            }
        };

        println!("limit={} count={} next_cursor={:?}", result.limit, result.count, result.next_cursor);
        for market in &result.data {
            // println!(
            //     "Page: {} Market: {} id={}",
            //     page, market.question, market.question_id
            // );
        }

        if let Some(next) = result.next_cursor {
            cursor = Some(next);
        } else {
            break;
        }
    }

    Ok(())
}
