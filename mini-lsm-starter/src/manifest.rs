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

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, ensure};
use bytes::{Buf, Bytes};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new().append(true).read(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut bytes: Bytes = buf.into();
        let mut records = Vec::new();
        while !bytes.is_empty() {
            let record_length = bytes.get_u32();
            let encoded_record = bytes.slice(..record_length as usize);
            bytes.advance(record_length as usize);
            let expected_checksum = bytes.get_u32();
            let checksum = crc32fast::hash(&encoded_record);
            ensure!(checksum == expected_checksum);

            records.push(serde_json::from_slice(&encoded_record)?);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();

        let encoded_record = serde_json::to_vec(&record)?;
        let checksum = crc32fast::hash(&encoded_record);
        file.write_all(&(encoded_record.len() as u32).to_be_bytes())?;
        file.write_all(&encoded_record)?;
        file.write_all(&checksum.to_be_bytes())?;

        file.sync_all()?;

        Ok(())
    }
}
