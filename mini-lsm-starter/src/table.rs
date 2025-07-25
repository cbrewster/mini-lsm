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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow, ensure};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

const SIZEOF_U32: usize = std::mem::size_of::<u32>();
const SIZEOF_U64: usize = std::mem::size_of::<u64>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.key_len() as u16);
            buf.put(meta.first_key.key_ref());
            buf.put_u64(meta.first_key.ts());
            buf.put_u16(meta.last_key.key_len() as u16);
            buf.put(meta.last_key.key_ref());
            buf.put_u64(meta.last_key.ts());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16();
            let first_key = buf.copy_to_bytes(first_key_len as usize);
            let first_key_ts = buf.get_u64();
            let last_key_len = buf.get_u16();
            let last_key = buf.copy_to_bytes(last_key_len as usize);
            let last_key_ts = buf.get_u64();
            block_meta.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes_with_ts(first_key, first_key_ts),
                last_key: KeyBytes::from_bytes_with_ts(last_key, last_key_ts),
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let size = file.size();

        let bloom_footer_size = 2 * SIZEOF_U32 as u64;
        let mut bloom_footer: Bytes = file
            .read(size - bloom_footer_size, bloom_footer_size)?
            .into();
        let expected_bloom_checksum = bloom_footer.get_u32();
        let bloom_offset = bloom_footer.get_u32() as u64;

        let encoded_bloom = file.read(bloom_offset, size - bloom_offset - bloom_footer_size)?;
        let bloom_checksum = crc32fast::hash(&encoded_bloom);
        ensure!(bloom_checksum == expected_bloom_checksum);
        let bloom = Bloom::decode(&encoded_bloom)?;

        let block_meta_footer_size = 2 * SIZEOF_U32 as u64 + SIZEOF_U64 as u64;
        let mut block_meta_footer: Bytes = file
            .read(
                bloom_offset - block_meta_footer_size,
                block_meta_footer_size,
            )?
            .into();
        let expected_block_meta_checksum = block_meta_footer.get_u32();
        let block_meta_offset = block_meta_footer.get_u32() as usize;
        let max_ts = block_meta_footer.get_u64();

        let encoded_block_meta = file.read(
            block_meta_offset as u64,
            bloom_offset - block_meta_offset as u64 - block_meta_footer_size,
        )?;
        let block_meta_checksum = crc32fast::hash(&encoded_block_meta);
        ensure!(block_meta_checksum == expected_block_meta_checksum);

        let block_meta = BlockMeta::decode_block_meta(encoded_block_meta.as_slice());

        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            bloom: Some(bloom),
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start_idx = self.block_meta[block_idx].offset as u64;
        let end_idx = self
            .block_meta
            .get(block_idx + 1)
            .map(|m| m.offset)
            .unwrap_or(self.block_meta_offset) as u64;
        assert!(end_idx > start_idx, "expected {end_idx} > {start_idx}");
        let encoded_block = &self.file.read(start_idx, end_idx - start_idx)?;
        let (encoded_block, mut expected_checksum_bytes) =
            encoded_block.split_at(encoded_block.len() - SIZEOF_U32);

        let expected_checksum = expected_checksum_bytes.get_u32();
        let checksum = crc32fast::hash(encoded_block);
        ensure!(checksum == expected_checksum);

        Ok(Arc::new(Block::decode(encoded_block)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        let Some(block_cache) = &self.block_cache else {
            return self.read_block(block_idx);
        };

        block_cache
            .try_get_with((self.id, block_idx), || self.read_block(block_idx))
            .map_err(|e| anyhow!("{}", e))
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
