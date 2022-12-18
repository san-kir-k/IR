use std::collections::{BinaryHeap, HashMap};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};

use mongodb::bson::oid::ObjectId;

use ordered_float::NotNan;

use crate::index;
use crate::inverted_index;

pub struct Engine {
    top_n: u64,
    forward_index: index::Index,
    inverted_index: inverted_index::InvertedIndex,
}

pub async fn init_engine() -> Result<Engine, Box<dyn std::error::Error>> {
    // TODO: add index load if index::is_index_built().await?

    let mut forward_index = index::create_index().await?;
    forward_index.build().await?;

    let mut inverted_index = inverted_index::create_inverted_index().await?;
    inverted_index.build().await?;

    Ok(Engine {
        top_n: 20,
        forward_index,
        inverted_index,
    })
}

impl Engine {
    pub async fn search(
        &self,
        query: Vec<String>,
    ) -> Result<Vec<ObjectId>, Box<dyn std::error::Error>> {
        let qvec = self.query_into_vec(&query);

        let mut result_of_bool_search: Option<Vec<Vec<u8>>> = None;

        for word in &query {
            let exists = self.inverted_index.head.get(word);
            match exists {
                Some(_) => {}
                None => {
                    continue;
                }
            }
            match result_of_bool_search {
                Some(intersec) => {
                    result_of_bool_search = Some(self.intersec(&intersec, word).await?);
                }
                None => {
                    result_of_bool_search = Some(self.get_posting_list(word).await?);
                }
            }
        }

        match result_of_bool_search {
            Some(documents) => {
                return Ok(self.get_best_documents(&qvec, documents).await?);
            }
            None => return Ok(vec![]),
        }
    }

    async fn get_best_documents(
        &self,
        qvec: &HashMap<String, f64>,
        documents: Vec<Vec<u8>>,
    ) -> Result<Vec<ObjectId>, Box<dyn std::error::Error>> {
        let mut result = Vec::default();
        let mut heap = BinaryHeap::new();

        for doc in documents {
            let dvec = self.doc_into_vec(&doc).await?;
            let mut cosine = 0.0;

            for word in qvec.keys() {
                if dvec.contains_key(word) {
                    cosine += qvec[word] * dvec[word];
                }
            }

            heap.push((NotNan::new(cosine).unwrap(), doc));
        }

        for _ in 0..self.top_n {
            if let Some((_, doc_id)) = heap.pop() {
                result.push(ObjectId::from_bytes(doc_id.try_into().unwrap()));
            } else {
                break;
            }
        }

        Ok(result)
    }

    async fn intersec(
        &self,
        curr: &Vec<Vec<u8>>,
        word: &String,
    ) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut result = Vec::default();

        let other = self.get_posting_list(word).await?;

        let (mut i, mut j) = (0, 0);

        while i < curr.len() && j < other.len() {
            if curr[i] == other[j] {
                result.push(curr[i].clone());
                i += 1;
                j += 1;
            } else if curr[i] < other[j] {
                i += 1;
            } else {
                j += 1;
            }
        }

        Ok(result)
    }

    async fn get_posting_list(
        &self,
        word: &String,
    ) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut result = Vec::default();

        let offset = self.inverted_index.head[word].0;

        let content_file = File::open(self.inverted_index.index_content_file).await?;
        let mut reader = BufReader::new(content_file);
        reader.seek(SeekFrom::Start(offset)).await?;

        let posting_list_len = reader.read_u64().await?;

        for _ in 0..posting_list_len {
            let mut bytes = vec![0u8; 12];
            reader.read_exact(&mut bytes).await?;
            result.push(bytes);
        }

        Ok(result)
    }

    fn query_into_vec(&self, query: &Vec<String>) -> HashMap<String, f64> {
        let mut tf = HashMap::new();
        for word in query {
            tf.entry(word.to_owned())
                .and_modify(|counter| *counter += 1.0)
                .or_insert(1.0);
        }
        for val in tf.values_mut() {
            *val = 1.0 + f64::log10(*val);
        }

        let keys = tf.keys().cloned().collect::<Vec<_>>();

        for word in keys {
            let default = (0, 0.0);
            let idf = self.inverted_index.head.get(&word).unwrap_or(&default).1;
            tf.entry(word).and_modify(|tf| *tf *= idf);
        }

        let len = f64::sqrt(tf.values().map(|&val| val * val).sum());
        for val in tf.values_mut() {
            *val /= len;
        }

        tf
    }

    async fn doc_into_vec(
        &self,
        doc_id: &Vec<u8>,
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        let mut tf = HashMap::new();

        let offset = self.forward_index.head[doc_id];

        let content_file = File::open(self.forward_index.index_content_file).await?;
        let mut reader = BufReader::new(content_file);
        reader.seek(SeekFrom::Start(offset)).await?;

        let words_count = reader.read_u64().await?;

        for _ in 0..words_count {
            let len = reader.read_u64().await?;
            let mut bytes = vec![0u8; len as usize];
            reader.read_exact(&mut bytes).await?;
            let tf_val = reader.read_f64().await?;
            tf.insert(String::from_utf8(bytes).unwrap(), tf_val);
        }

        let keys = tf.keys().cloned().collect::<Vec<_>>();

        for word in keys {
            let default = (0, 0.0);
            let idf = self.inverted_index.head.get(&word).unwrap_or(&default).1;
            tf.entry(word).and_modify(|tf| *tf *= idf);
        }

        let len = f64::sqrt(tf.values().map(|&val| val * val).sum());
        for val in tf.values_mut() {
            *val /= len;
        }

        Ok(tf)
    }
}
