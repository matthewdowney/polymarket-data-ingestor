//! This script is used to download the markets from the Polymarket API and write them to a file.
use std::{fs::File, io::Write, path::Path};
use prediction_data_ingestor::{MARKETS_FILE, ApiResponse};

use anyhow::Result;
use reqwest::Client;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new();
    let mut cursor: Option<String> = None;
    let mut data = Vec::new();

    loop {
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

        println!(
            "limit={} count={} next_cursor={:?}",
            result.limit, result.count, result.next_cursor
        );
        data.extend(result.data);

        match result.next_cursor {
            Some(next) if result.count == result.limit => cursor = Some(next),
            // if no next cursor or count is less than limit, we've reached the end
            _ => break,
        }
    }

    println!("found {} markets, writing to markets.ndjson", data.len());

    // write markets to one json file
    {
        let path = Path::new(MARKETS_FILE);
        if path.exists() {
            std::fs::rename(path, path.with_extension(".ndjson.bak"))?;
        }
    }

    let mut file = File::create(MARKETS_FILE)?;
    for market in data {
        serde_json::to_writer(&mut file, &market)?;
        file.write_all(b"\n")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};

    use prediction_data_ingestor::PolymarketMarket;

    use super::*;

    #[test]
    fn test_deserialize_market() -> anyhow::Result<()> {
        // read markets.json
        let file = File::open("markets.ndjson")?;

        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        let mut line = String::new();

        while reader.read_line(&mut line)? > 0 {
            let market: PolymarketMarket = serde_json::from_str(&line)?;
            data.push(market);
            line.clear();
        }

        // check that the data is valid
        assert!(data.len() > 0);
        assert!(data[0].question.len() > 0);

        Ok(())
    }
}
