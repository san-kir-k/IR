use std::collections::HashMap;
use std::mem::size_of;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ErrorKind};

use futures::stream::StreamExt;

use mongodb::bson;

use crate::db;

pub struct Index {
    pub index_head_file: &'static str,
    pub index_content_file: &'static str,
    next_offset: usize,
    pub head: HashMap<Vec<u8>, u64>,
}

pub fn is_index_built() -> Result<bool, Box<dyn std::error::Error>> {
    if std::path::Path::new("idx/head.idx.bin").exists()
        && std::path::Path::new("idx/content.idx.bin").exists()
    {
        return Ok(true);
    } else {
        return Ok(false);
    }
}

pub async fn create_index() -> Result<Index, Box<dyn std::error::Error>> {
    let index_head_file = "idx/head.idx.bin";
    let index_content_file = "idx/content.idx.bin";

    Ok(Index {
        index_head_file,
        index_content_file,
        next_offset: 0,
        head: HashMap::default(),
    })
}

impl Index {
    pub async fn load(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let head_file = File::open(self.index_head_file).await?;
        let mut reader = BufReader::new(head_file);

        loop {
            let mut bytes = vec![0u8; 12];
            let res = reader.read_exact(&mut bytes).await;
            if res.is_err() {
                if res.expect_err("Expected error!").kind() == ErrorKind::UnexpectedEof {
                    break;
                } else {
                    panic!("Unexpected error reading file.");
                }
            }
            let offset = reader.read_u64().await?;

            self.head.insert(bytes, offset);
        }

        print!("Index loaded\n");

        Ok(())
    }

    pub async fn build(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let dbase = db::connect_to_docs_database().await?;
        let mut cur = dbase.get_cursor().await?;

        let index_head = File::create(self.index_head_file).await?;
        let mut index_head_writer = BufWriter::new(index_head);

        let index_content = File::create(self.index_content_file).await?;
        let mut index_content_writer = BufWriter::new(index_content);

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

            self.write_doc_to_disk(
                &tf,
                words_count,
                &doc,
                &mut index_head_writer,
                &mut index_content_writer,
            )
            .await?;
        }

        print!("Index was built\n");

        Ok(())
    }

    async fn write_doc_to_disk(
        &mut self,
        tf: &HashMap<&String, f64>,
        words_count: u64,
        doc: &db::Doc,
        index_head_writer: &mut BufWriter<File>,
        index_content_writer: &mut BufWriter<File>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.head
            .insert(doc._id.bytes().to_vec(), self.next_offset as u64);

        index_head_writer.write(&doc._id.bytes()).await?;
        index_head_writer.write_u64(self.next_offset as u64).await?;

        index_content_writer.write_u64(words_count).await?;
        self.next_offset += size_of::<u64>();

        for word in &doc.words {
            index_content_writer.write_u64(word.len() as u64).await?;
            self.next_offset += size_of::<u64>();

            index_content_writer.write(word.as_bytes()).await?;
            self.next_offset += word.as_bytes().len() * size_of::<u8>();

            index_content_writer.write_f64(tf[word]).await?;
            self.next_offset += size_of::<f64>();
        }

        index_head_writer.flush().await?;
        index_content_writer.flush().await?;

        Ok(())
    }
}
