use serde::{Deserialize, Serialize};

use mongodb::options::ClientOptions;
use mongodb::{bson, bson::doc, bson::oid::ObjectId};
use mongodb::{Client, Collection, Cursor};

#[derive(Debug, Serialize, Deserialize)]
pub struct Doc {
    pub _id: ObjectId,
    pub title: String,
    pub path: String,
    pub words: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Block {
    pub block_id: usize,
    pub batch_id: usize,
    pub word: String,
    pub doc_ids: Vec<ObjectId>,
}

pub struct DocumentsDB {
    docs_collection: Collection<Doc>,
}

pub async fn connect_to_docs_database() -> Result<DocumentsDB, Box<dyn std::error::Error>> {
    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    let db = client.database("IR");
    let docs_collection = db.collection::<Doc>("DocsStorage");

    Ok(DocumentsDB { docs_collection })
}

impl DocumentsDB {
    pub async fn get_cursor(&self) -> Result<Cursor<bson::Document>, Box<dyn std::error::Error>> {
        let pipeline = vec![doc! {
            "$sort": {
                "_id": 1
            }
        }];
        let cursor = self.docs_collection.aggregate(pipeline, None).await?;
        Ok(cursor)
    }

    pub async fn get_count_of_documents(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let count = self.docs_collection.count_documents(doc! {}, None).await?;
        Ok(count)
    }
}
