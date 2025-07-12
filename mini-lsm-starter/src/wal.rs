// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Result, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(File::create(path)?))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().append(true).read(true).open(path)?;

        loop {
            let mut body_len_bytes = [0u8; 4];
            if let Err(error) = file.read_exact(&mut body_len_bytes[..]) {
                if error.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(error.into());
            }
            let body_len = u32::from_be_bytes(body_len_bytes);

            let mut body = vec![0u8; body_len as usize];
            file.read_exact(&mut body)?;
            let mut cursor = &body[..];

            let key_len = cursor.get_u16();
            let key = Bytes::copy_from_slice(&cursor[..key_len as usize]);
            cursor.advance(key_len as usize);

            let key_ts = cursor.get_u64();

            let value_len = cursor.get_u16();
            let value = Bytes::copy_from_slice(&cursor[..value_len as usize]);

            let mut expected_checksum_bytes = [0u8; 4];
            file.read_exact(&mut expected_checksum_bytes[..])?;
            let expected_checksum = u32::from_be_bytes(expected_checksum_bytes);

            let checksum = crc32fast::hash(&body);
            ensure!(checksum == expected_checksum);

            skiplist.insert(KeyBytes::from_bytes_with_ts(key, key_ts), value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut body = BytesMut::new();
        for (key, value) in data {
            body.put_u16(key.key_len() as u16);
            body.put(key.key_ref());
            body.put_u64(key.ts());
            body.put_u16(value.len() as u16);
            body.put(*value);
        }
        let checksum = crc32fast::hash(&body);
        let body_size = body.len() as u32;

        let mut file = self.file.lock();
        file.write_all(&body_size.to_be_bytes())?;
        file.write_all(&body)?;
        file.write_all(&checksum.to_be_bytes())?;
        file.flush()?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.lock().get_mut().sync_all()?;
        Ok(())
    }
}
