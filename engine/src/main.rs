pub mod db;
pub mod engine;
pub mod index;
pub mod inverted_index;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = engine::init_engine().await?;
    let query = vec![
        "relief".to_string(),
        "pitcher".to_string(),
        "game".to_string(),
        "pitch".to_string(),
    ];
    let res = engine.search(query).await?;

    for oid in res {
        print!("{:?}\n", oid.to_hex());
    }

    Ok(())
}
