use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::mem::size_of;

use tokio::fs::{copy, create_dir, remove_dir_all, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom, AsyncSeekExt, ErrorKind};

use futures::future::join_all;
use futures::stream::StreamExt;

use mongodb::bson::{self, oid::ObjectId};

use crate::db;

pub struct InvertedIndex {
    index_head_file: &'static str,
    index_content_file: &'static str,
    max_map_size: usize,
    cur_map_size: usize,
    count_of_written_blocks: usize,
    blocks_directory: &'static str,
    head: HashMap<String, (u64, f64)>,
}

pub async fn is_inverted_index_built() -> Result<bool, Box<dyn std::error::Error>> {
    let index_head_file = File::open("inv_idx/head.inv_idx.bin").await;
    let index_content_file = File::open("inv_idx/content.inv_idx.bin").await;

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

pub async fn create_inverted_index() -> Result<InvertedIndex, Box<dyn std::error::Error>> {
    let index_head_file = "inv_idx/head.inv_idx.bin";
    let index_content_file = "inv_idx/content.inv_idx.bin";

    remove_dir_all("inv_idx/blocks").await?;
    create_dir("inv_idx/blocks").await?;

    Ok(InvertedIndex {
        index_head_file,
        index_content_file,
        max_map_size: 10_000,
        cur_map_size: 0,
        count_of_written_blocks: 0,
        blocks_directory: "inv_idx/blocks",
        head: HashMap::default(),
    })
}

impl InvertedIndex {
    pub async fn build(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let dbase = db::connect_to_docs_database().await?;
        let mut cur = dbase.get_cursor().await?;

        let mut inverted_idx_block = BTreeMap::new();

        let mut tasks = Vec::new();

        while let Some(result) = cur.next().await {
            let doc: db::Doc = bson::from_document(result?)?;
            let mut used = HashSet::new();
            for word in doc.words {
                used.insert(word.to_owned());
                if !inverted_idx_block.contains_key(&word) {
                    inverted_idx_block.insert(word, vec![doc._id]);
                } else {
                    if let Some(posting_list) = inverted_idx_block.get_mut(&word) {
                        posting_list.push(doc._id);
                    }
                }

                self.cur_map_size += 1;

                if self.cur_map_size >= self.max_map_size {
                    print!("Block {:?} dumped\n", self.count_of_written_blocks);
                    tasks.push(InvertedIndex::write_block_to_disk(
                        inverted_idx_block,
                        self.count_of_written_blocks,
                        self.blocks_directory,
                    ));
                    self.count_of_written_blocks += 1;
                    inverted_idx_block = BTreeMap::new();
                    self.cur_map_size = 0;
                }
            }

            for word in used {
                self.head
                    .entry(word)
                    .and_modify(|pair| *pair = (pair.0, pair.1 + 1.0))
                    .or_insert((0, 1.0));
            }
        }

        if inverted_idx_block.len() > 0 {
            print!("EXTRA Block {:?} dumped\n", self.count_of_written_blocks);
            tasks.push(InvertedIndex::write_block_to_disk(
                inverted_idx_block,
                self.count_of_written_blocks,
                self.blocks_directory,
            ));
            self.count_of_written_blocks += 1;
        }

        let results = join_all(tasks).await;
        for res in results {
            res?;
        }
        print!("Joined\n");

        self.merge_blocks().await?;

        let mut resulting_block_file = self.blocks_directory.to_owned();
        resulting_block_file.push_str(&format!(
            "/block_{}.inv_idx.bin",
            self.count_of_written_blocks - 1
        ));
        copy(resulting_block_file, self.index_content_file).await?;
        print!("Inverted index's content file was built\n");

        let count_of_documents = dbase.get_count_of_documents().await?;
        for pair in self.head.values_mut() {
            *pair = (pair.0, f64::log10(count_of_documents as f64 / pair.1));
        }

        self.set_offsets().await?;
        self.write_head_to_disk().await?;
        print!("Inverted index was built\n");

        Ok(())
    }

    async fn write_block_to_disk(
        inverted_idx_block: BTreeMap<String, Vec<ObjectId>>,
        block_number: usize,
        block_directory: &'static str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut block_file = block_directory.to_owned();
        block_file.push_str(&format!("/block_{}.inv_idx.bin", block_number));

        let index_block = File::create(block_file).await?;
        let mut index_block_writer = BufWriter::new(index_block);

        for (word, mut posting_list) in inverted_idx_block {
            posting_list.dedup();
            InvertedIndex::write_inverted_index_record_to_disk(
                &mut index_block_writer,
                &word,
                &posting_list,
            )
            .await?;
        }

        index_block_writer.flush().await?;

        Ok(())
    }

    async fn merge_blocks(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut blocks_deq = VecDeque::new();
        for block_num in 0..self.count_of_written_blocks {
            blocks_deq.push_back(block_num);
        }

        while !blocks_deq.is_empty() {
            let lhs = blocks_deq.pop_front();
            let rhs = blocks_deq.pop_front();
            if lhs.is_none() || rhs.is_none() {
                break;
            }
            print!(
                "Blocks {:?} and {:?} going to be merged...\n",
                lhs.unwrap(),
                rhs.unwrap()
            );
            self.merge_task(lhs.unwrap(), rhs.unwrap()).await?;
            blocks_deq.push_back(self.count_of_written_blocks);
            print!("Block {:?} added\n", self.count_of_written_blocks);
            self.count_of_written_blocks += 1;
        }

        Ok(())
    }

    async fn set_offsets(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut next_offset: u64 = 0;
        let content_file = File::open(self.index_content_file).await?;
        let mut reader = BufReader::new(content_file);

        loop {
            let word_len_res = reader.read_u64().await;
            if word_len_res.is_err() {
                if word_len_res.expect_err("Expected error!").kind() == ErrorKind::UnexpectedEof
                {
                    break;
                } else {
                    panic!("Unexpected error reading file.");
                }
            }
            let word_len = word_len_res.unwrap();
            let mut word_bytes = vec![0u8; word_len as usize];
            reader.read_exact(&mut word_bytes).await?;
            let posting_list_len = reader.read_u64().await?;
            
            next_offset += size_of::<u64>() as u64 + (size_of::<u8>() as u64) * word_len + size_of::<u64>() as u64;
            self.head
                .entry(String::from_utf8(word_bytes).unwrap())
                .and_modify(|pair| *pair = (next_offset, pair.1 + 1.0));

            next_offset += (size_of::<u8>() as u64) * 12 * posting_list_len;
            reader.seek(SeekFrom::Start(next_offset)).await?;
        }

        Ok(())
    }

    async fn write_head_to_disk(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let index_head = File::create(self.index_head_file).await?;
        let mut index_head_writer = BufWriter::new(index_head);

        for (word, (offset, idf)) in &self.head {
            index_head_writer.write(word.as_bytes()).await?;
            index_head_writer.write_u64(*offset as u64).await?;
            index_head_writer.write_f64(*idf).await?;
        }

        index_head_writer.flush().await?;

        Ok(())
    }

    async fn write_inverted_index_record_to_disk(
        writer: &mut BufWriter<File>,
        word: &String,
        posting_list: &Vec<ObjectId>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writer.write_u64(word.len() as u64).await?;
        writer.write(word.as_bytes()).await?;

        writer.write_u64(posting_list.len() as u64).await?;
        for doc_id in posting_list {
            writer.write(&doc_id.bytes()).await?;
        }

        Ok(())
    }

    async fn rewrite_record(
        writer: &mut BufWriter<File>,
        word: &Vec<u8>,
        len: u64,
        reader: &mut BufReader<File>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writer.write_u64(word.len() as u64).await?;
        writer.write(word).await?;

        writer.write_u64(len).await?;
        for _ in 0..len {
            let mut bytes = vec![0u8; 12];
            reader.read_exact(&mut bytes).await?;
            writer.write(&bytes).await?;
        }

        writer.flush().await?;

        Ok(())
    }

    async fn merge_records(
        writer: &mut BufWriter<File>,
        word: &Vec<u8>,
        mut lhs_len: u64,
        lhs_reader: &mut BufReader<File>,
        mut rhs_len: u64,
        rhs_reader: &mut BufReader<File>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writer.write_u64(word.len() as u64).await?;
        writer.write(word).await?;

        writer.write_u64(lhs_len + rhs_len).await?;

        let mut lhs_bytes = vec![0u8; 12];
        let mut rhs_bytes = vec![0u8; 12];

        lhs_reader.read_exact(&mut lhs_bytes).await?;
        rhs_reader.read_exact(&mut rhs_bytes).await?;
        let mut lhs_written_count = lhs_len;
        let mut rhs_written_count = rhs_len;
        lhs_len -= 1;
        rhs_len -= 1;
        loop {
            if lhs_bytes < rhs_bytes {
                writer.write(&lhs_bytes).await?;
                lhs_written_count -= 1;
                if lhs_len == 0 {
                    break;
                }
                lhs_reader.read_exact(&mut lhs_bytes).await?;
                lhs_len -= 1;
            } else {
                writer.write(&rhs_bytes).await?;
                rhs_written_count -= 1;
                if rhs_len == 0 {
                    break;
                }
                rhs_reader.read_exact(&mut rhs_bytes).await?;
                rhs_len -= 1;
            }
        }

        if lhs_written_count != 0 {
            writer.write(&lhs_bytes).await?;
        }
        if rhs_written_count != 0 {
            writer.write(&rhs_bytes).await?;
        }

        if lhs_len == 0 && rhs_len != 0 {
            for _ in 0..rhs_len {
                let mut bytes = vec![0u8; 12];
                rhs_reader.read_exact(&mut bytes).await?;
                writer.write(&bytes).await?;
            }
        } else if lhs_len != 0 && rhs_len == 0 {
            for _ in 0..lhs_len {
                let mut bytes = vec![0u8; 12];
                lhs_reader.read_exact(&mut bytes).await?;
                writer.write(&bytes).await?;
            }
        }

        writer.flush().await?;

        Ok(())
    }

    async fn write_tail(
        writer: &mut BufWriter<File>,
        mut word: Vec<u8>,
        mut len: u64,
        reader: &mut BufReader<File>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            InvertedIndex::rewrite_record(writer, &word, len, reader).await?;

            let word_len = reader.read_u64().await;
            if word_len.is_err() {
                if word_len.expect_err("Expected error!").kind() == ErrorKind::UnexpectedEof {
                    break;
                } else {
                    panic!("Unexpected error reading file.");
                }
            }
            word = vec![0u8; word_len.unwrap() as usize];
            reader.read_exact(&mut word).await?;
            len = reader.read_u64().await?;
        }

        writer.flush().await?;

        Ok(())
    }

    async fn merge_task(
        &mut self,
        lhs_block_num: usize,
        rhs_block_num: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // ----------------------- init file readers and resulting writer ---------------------
        let mut lhs_filename = self.blocks_directory.to_owned();
        lhs_filename.push_str(&format!("/block_{}.inv_idx.bin", lhs_block_num));
        let lhs_file = File::open(lhs_filename).await?;
        let mut lhs_reader = BufReader::new(lhs_file);

        let mut rhs_filename = self.blocks_directory.to_owned();
        rhs_filename.push_str(&format!("/block_{}.inv_idx.bin", rhs_block_num));
        let rhs_file = File::open(rhs_filename).await?;
        let mut rhs_reader = BufReader::new(rhs_file);

        let mut res_filename = self.blocks_directory.to_owned();
        res_filename.push_str(&format!(
            "/block_{}.inv_idx.bin",
            self.count_of_written_blocks
        ));
        let res_file = File::create(res_filename).await?;
        let mut res_writer = BufWriter::new(res_file);
        // -------------------------------------------------------------------------------------

        let mut lhs_word_len = lhs_reader.read_u64().await;
        let mut lhs_word_bytes = vec![0u8; lhs_word_len.unwrap() as usize];
        lhs_reader.read_exact(&mut lhs_word_bytes).await?;
        let mut lhs_posting_list_len = lhs_reader.read_u64().await?;

        let mut rhs_word_len = rhs_reader.read_u64().await;
        let mut rhs_word_bytes = vec![0u8; rhs_word_len.unwrap() as usize];
        rhs_reader.read_exact(&mut rhs_word_bytes).await?;
        let mut rhs_posting_list_len = rhs_reader.read_u64().await?;

        loop {
            if lhs_word_bytes < rhs_word_bytes {
                InvertedIndex::rewrite_record(
                    &mut res_writer,
                    &lhs_word_bytes,
                    lhs_posting_list_len,
                    &mut lhs_reader,
                )
                .await?;

                lhs_word_len = lhs_reader.read_u64().await;
                if lhs_word_len.is_err() {
                    if lhs_word_len.expect_err("Expected error!").kind() == ErrorKind::UnexpectedEof
                    {
                        InvertedIndex::write_tail(
                            &mut res_writer,
                            rhs_word_bytes,
                            rhs_posting_list_len,
                            &mut rhs_reader,
                        )
                        .await?;

                        break;
                    } else {
                        panic!("Unexpected error reading file.");
                    }
                }
                lhs_word_bytes = vec![0u8; lhs_word_len.unwrap() as usize];
                lhs_reader.read_exact(&mut lhs_word_bytes).await?;
                lhs_posting_list_len = lhs_reader.read_u64().await?;
            } else if lhs_word_bytes > rhs_word_bytes {
                InvertedIndex::rewrite_record(
                    &mut res_writer,
                    &rhs_word_bytes,
                    rhs_posting_list_len,
                    &mut rhs_reader,
                )
                .await?;

                rhs_word_len = rhs_reader.read_u64().await;
                if rhs_word_len.is_err() {
                    if rhs_word_len.expect_err("Expected error!").kind() == ErrorKind::UnexpectedEof
                    {
                        InvertedIndex::write_tail(
                            &mut res_writer,
                            lhs_word_bytes,
                            lhs_posting_list_len,
                            &mut lhs_reader,
                        )
                        .await?;

                        break;
                    } else {
                        panic!("Unexpected error reading file.");
                    }
                }
                rhs_word_bytes = vec![0u8; rhs_word_len.unwrap() as usize];
                rhs_reader.read_exact(&mut rhs_word_bytes).await?;
                rhs_posting_list_len = rhs_reader.read_u64().await?;
            } else {
                InvertedIndex::merge_records(
                    &mut res_writer,
                    &lhs_word_bytes,
                    lhs_posting_list_len,
                    &mut lhs_reader,
                    rhs_posting_list_len,
                    &mut rhs_reader,
                )
                .await?;

                lhs_word_len = lhs_reader.read_u64().await;
                if lhs_word_len.is_err() {
                    if lhs_word_len.expect_err("Expected error!").kind() == ErrorKind::UnexpectedEof
                    {
                        rhs_word_len = rhs_reader.read_u64().await;
                        if rhs_word_len.is_err() {
                            if rhs_word_len.expect_err("Expected error!").kind()
                                == ErrorKind::UnexpectedEof
                            {
                                break;
                            } else {
                                panic!("Unexpected error reading file.");
                            }
                        }
                        rhs_word_bytes = vec![0u8; rhs_word_len.unwrap() as usize];
                        rhs_reader.read_exact(&mut rhs_word_bytes).await?;
                        rhs_posting_list_len = rhs_reader.read_u64().await?;

                        InvertedIndex::write_tail(
                            &mut res_writer,
                            rhs_word_bytes,
                            rhs_posting_list_len,
                            &mut rhs_reader,
                        )
                        .await?;

                        break;
                    } else {
                        panic!("Unexpected error reading file.");
                    }
                }
                lhs_word_bytes = vec![0u8; lhs_word_len.unwrap() as usize];
                lhs_reader.read_exact(&mut lhs_word_bytes).await?;
                lhs_posting_list_len = lhs_reader.read_u64().await?;

                rhs_word_len = rhs_reader.read_u64().await;
                if rhs_word_len.is_err() {
                    if rhs_word_len.expect_err("Expected error!").kind() == ErrorKind::UnexpectedEof
                    {
                        InvertedIndex::write_tail(
                            &mut res_writer,
                            lhs_word_bytes,
                            lhs_posting_list_len,
                            &mut lhs_reader,
                        )
                        .await?;

                        break;
                    } else {
                        panic!("Unexpected error reading file.");
                    }
                }
                rhs_word_bytes = vec![0u8; rhs_word_len.unwrap() as usize];
                rhs_reader.read_exact(&mut rhs_word_bytes).await?;
                rhs_posting_list_len = rhs_reader.read_u64().await?;
            }
        }

        Ok(())
    }
}
