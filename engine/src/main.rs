pub mod db;
pub mod index;
pub mod inverted_index;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !index::is_index_built().await? {
        let mut index = index::create_index().await?;
        index.build().await?;
    }

    if !inverted_index::is_inverted_index_built().await? {
        let mut inv_index = inverted_index::create_inverted_index().await?;
        inv_index.build().await?;
    }

    Ok(())
}
