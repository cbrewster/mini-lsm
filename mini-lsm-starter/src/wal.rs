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

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(File::create(path)?))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().append(true).read(true).open(path)?;

        loop {
            let mut key_len_bytes = [0u8; 2];
            if let Err(error) = file.read_exact(&mut key_len_bytes[..]) {
                if error.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(error.into());
            }
            let key_len = u16::from_be_bytes(key_len_bytes);

            let mut key = vec![0u8; key_len as usize];
            file.read_exact(&mut key[..])?;

            let mut value_len_bytes = [0u8; 2];
            file.read_exact(&mut value_len_bytes[..])?;
            let value_len = u16::from_be_bytes(value_len_bytes);

            let mut value = vec![0u8; value_len as usize];
            file.read_exact(&mut value[..])?;

            skiplist.insert(key.into(), value.into());
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        file.write_all(&(key.len() as u16).to_be_bytes())?;
        file.write_all(key)?;
        file.write_all(&(value.len() as u16).to_be_bytes())?;
        file.write_all(value)?;
        file.flush()?;
        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        self.file.lock().get_mut().sync_all()?;
        Ok(())
    }
}
