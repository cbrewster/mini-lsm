// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
    table::{FileObject, bloom::Bloom},
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    max_ts: u64,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            max_ts: 0,
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));

        if key.ts() > self.max_ts {
            self.max_ts = key.ts();
        }

        if self.builder.add(key, value) {
            if self.first_key.is_empty() {
                self.first_key.set_from_slice(key);
            }
            self.last_key.set_from_slice(key);
            return;
        }
        self.finish_block();

        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
        self.max_ts = key.ts();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        let checksum = crc32fast::hash(&encoded_block);
        self.data.extend(encoded_block);
        self.data.put_u32(checksum);
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();

        let mut buf = self.data;
        let block_meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        let block_meta_checksum = crc32fast::hash(&buf[block_meta_offset..]);
        buf.put_u32(block_meta_checksum);
        buf.put_u32(block_meta_offset as u32);
        buf.put_u64(self.max_ts);

        let bloom_offset = buf.len();
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        bloom.encode(&mut buf);
        let bloom_checksum = crc32fast::hash(&buf[bloom_offset..]);
        buf.put_u32(bloom_checksum);
        buf.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
