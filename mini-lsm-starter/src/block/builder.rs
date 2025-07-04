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

use bytes::BufMut;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_overlap(first_key: KeySlice, key: KeySlice) -> u16 {
    let mut n = 0;
    for (a, b) in first_key.raw_ref().iter().zip(key.raw_ref().iter()) {
        if a != b {
            break;
        }
        n += 1
    }
    n
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        let overlap = if self.is_empty() {
            self.first_key = key.to_key_vec();
            0
        } else {
            let estimated_size = key.len() + value.len() + 3 * SIZEOF_U16;
            let current_size = self.offsets.len() * SIZEOF_U16 + self.data.len() + SIZEOF_U16;
            if estimated_size + current_size >= self.block_size {
                return false;
            }

            compute_overlap(self.first_key.as_key_slice(), key)
        };

        let offset = self.data.len() as u16;
        self.offsets.push(offset);

        self.data.put_u16(overlap);
        self.data.put_u16(key.len() as u16 - overlap);
        self.data.put(&key.raw_ref()[overlap as usize..]);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
