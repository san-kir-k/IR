use std::collections::HashMap;
use std::mem::size_of;

use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

use futures::stream::StreamExt;

use mongodb::bson;

use crate::db;

pub struct Index {
    index_head_writer: BufWriter<File>,
    index_content_writer: BufWriter<File>,
    next_offset: usize,
}

pub async fn is_index_built() -> Result<bool, Box<dyn std::error::Error>> {
    let index_head_file = File::open("idx/head.idx.bin").await;
    let index_content_file = File::open("idx/content.idx.bin").await;

    if index_head_file.is_err() || index_content_file.is_err() {
        return Ok(false);
    }

    if index_head_file?.metadata().await?.len() > 0
        && index_content_file?.metadata().await?.len() > 0
    {
        return Ok(true);
    } else {
        return Ok(false);
    }
}

pub async fn create_index() -> Result<Index, Box<dyn std::error::Error>> {
    let index_head_file = "idx/head.idx.bin";
    let index_content_file = "idx/content.idx.bin";

    let index_head = File::create(index_head_file).await?;
    let index_head_writer = BufWriter::new(index_head);

    let index_content = File::create(index_content_file).await?;
    let index_content_writer = BufWriter::new(index_content);

    Ok(Index {
        index_head_writer,
        index_content_writer,
        next_offset: 0,
    })
}

impl Index {
    pub async fn build(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let dbase = db::connect_to_docs_database().await?;
        let mut cur = dbase.get_cursor().await?;

        while let Some(result) = cur.next().await {
            let mut tf = HashMap::new();
            let doc: db::Doc = bson::from_document(result?)?;
            let mut words_count: u64 = 0;
            for word in &doc.words {
                tf.entry(word)
                    .and_modify(|counter| *counter += 1.0)
                    .or_insert(1.0);
                words_count += 1;
            }
            for val in tf.values_mut() {
                *val = 1.0 + f64::log10(*val);
            }

            self.write_doc_to_disk(&tf, words_count, &doc).await?;
        }

        Ok(())
    }

    async fn write_doc_to_disk(
        &mut self,
        tf: &HashMap<&String, f64>,
        words_count: u64,
        doc: &db::Doc,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.index_head_writer.write(&doc._id.bytes()).await?;
        self.index_head_writer
            .write_u64(self.next_offset as u64)
            .await?;

        self.index_content_writer.write_u64(words_count).await?;
        self.next_offset += size_of::<u64>();

        for word in &doc.words {
            self.index_content_writer
                .write_u64(word.len() as u64)
                .await?;
            self.next_offset += size_of::<u64>();

            self.index_content_writer.write(word.as_bytes()).await?;
            self.next_offset += word.as_bytes().len() * size_of::<u8>();

            self.index_content_writer.write_f64(tf[word]).await?;
            self.next_offset += size_of::<f64>();
        }

        self.index_head_writer.flush().await?;
        self.index_content_writer.flush().await?;

        Ok(())
    }
}
