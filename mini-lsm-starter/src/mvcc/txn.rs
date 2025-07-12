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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, bail, ensure};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mvcc::CommittedTxnData,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("txn already committed");
        }
        if let Some(entry) = self.local_storage.get(key) {
            return if entry.value().is_empty() {
                Ok(None)
            } else {
                Ok(Some(entry.value().clone()))
            };
        }

        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().1.insert(farmhash::hash32(key));
        }

        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("txn already committed");
        }
        let mut local_iter = TxnLocalIterator::new(
            self.local_storage.clone(),
            |map| {
                map.range((
                    lower.map(Bytes::copy_from_slice),
                    upper.map(Bytes::copy_from_slice),
                ))
            },
            (Bytes::new(), Bytes::new()),
        );
        local_iter.next().unwrap();

        TxnIterator::create(
            self.clone(),
            FusedIterator::new(TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?),
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("txn already committed");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(farmhash::hash32(key));
        }
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("txn already committed");
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(farmhash::hash32(key));
        }
    }

    pub fn commit(&self) -> Result<()> {
        ensure!(
            !self.committed.swap(true, Ordering::SeqCst),
            "already committed"
        );

        let mvcc = self.inner.mvcc();
        let _commit_lock = mvcc.commit_lock.lock();

        if let Some(key_hashes) = &self.key_hashes {
            let guard = key_hashes.lock();
            let (write_set, read_set) = &*guard;
            let expected_commit_ts = mvcc.latest_commit_ts();

            if !write_set.is_empty() {
                let commited_txns = mvcc.committed_txns.lock();
                for (_, txn_data) in commited_txns.range((self.read_ts + 1)..) {
                    for key_hash in read_set {
                        if txn_data.key_hashes.contains(key_hash) {
                            bail!("failed to serialize transactions");
                        }
                    }
                }
            }
        }

        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();

        let commit_ts = self.inner.write_batch_inner(&batch)?;

        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let (write_set, _) = &mut *guard;
            mvcc.committed_txns.lock().insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );

            let mut commited_txns = mvcc.committed_txns.lock();
            let watermark = mvcc.watermark();
            while let Some(entry) = commited_txns.first_entry() {
                if *entry.key() >= watermark {
                    break;
                }
                entry.remove();
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| {
            iter.next()
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or((Bytes::new(), Bytes::new()))
        });
        self.with_item_mut(|item| *item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: FusedIterator<TwoMergeIterator<TxnLocalIterator, LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: FusedIterator<TwoMergeIterator<TxnLocalIterator, LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = TxnIterator { txn, iter };
        iter.skip_deletes()?;
        if iter.is_valid() {
            iter.add_to_read_set(iter.key());
        }
        Ok(iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(key_hashes) = &self.txn.key_hashes {
            key_hashes.lock().1.insert(farmhash::hash32(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()?;
        if self.is_valid() {
            self.add_to_read_set(self.key());
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
