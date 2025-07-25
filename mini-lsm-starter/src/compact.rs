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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_iters = l0_sstables
                    .iter()
                    .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                    .map(|table| SsTableIterator::create_and_seek_to_first(table).map(Box::new))
                    .collect::<Result<Vec<_>>>()?;
                let l1_tables = l1_sstables
                    .iter()
                    .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                    .collect::<Vec<_>>();
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_tables)?,
                )?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }

            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            }) => {
                if upper_level.is_some() {
                    let mut upper_tables = Vec::with_capacity(upper_level_sst_ids.len());
                    for sst_id in upper_level_sst_ids {
                        upper_tables.push(snapshot.sstables.get(sst_id).unwrap().clone());
                    }
                    let mut lower_tables = Vec::with_capacity(lower_level_sst_ids.len());
                    for sst_id in lower_level_sst_ids {
                        lower_tables.push(snapshot.sstables.get(sst_id).unwrap().clone());
                    }
                    let iter = TwoMergeIterator::create(
                        SstConcatIterator::create_and_seek_to_first(upper_tables)?,
                        SstConcatIterator::create_and_seek_to_first(lower_tables)?,
                    )?;
                    self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
                } else {
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for sst_id in upper_level_sst_ids {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(sst_id).unwrap().clone(),
                        )?));
                    }
                    let mut lower_tables = Vec::with_capacity(lower_level_sst_ids.len());
                    for sst_id in lower_level_sst_ids {
                        lower_tables.push(snapshot.sstables.get(sst_id).unwrap().clone());
                    }
                    let iter = TwoMergeIterator::create(
                        MergeIterator::create(upper_iters),
                        SstConcatIterator::create_and_seek_to_first(lower_tables)?,
                    )?;
                    self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
                }
            }
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let mut iters = Vec::with_capacity(tiers.len());
                for (_, level_sst_ids) in tiers {
                    let level_tables = level_sst_ids
                        .iter()
                        .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                        .collect();
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        level_tables,
                    )?));
                }
                let iter = MergeIterator::create(iters);
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
        }
    }

    fn compact_generate_sst_from_iter<I>(
        &self,
        mut iter: I,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>>
    where
        I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    {
        let mut ssts = Vec::new();
        let mut builder = None;
        let mut last_key = Vec::new();

        let watermark = self.mvcc().watermark();
        println!("watermark {watermark} bottom {compact_to_bottom_level}");

        let compaction_filters = self.compaction_filters.lock().clone();
        let mut first_key_below_watermark = false;
        while iter.is_valid() {
            let same_as_last_key = iter.key().key_ref() == last_key;
            if !same_as_last_key {
                first_key_below_watermark = true;
            }

            if compact_to_bottom_level
                && !same_as_last_key
                && iter.key().ts() <= watermark
                && iter.value().is_empty()
            {
                last_key.clear();
                last_key.extend_from_slice(iter.key().key_ref());
                iter.next()?;
                first_key_below_watermark = false;
                continue;
            }

            if iter.key().ts() <= watermark {
                if !first_key_below_watermark {
                    if !same_as_last_key {
                        last_key.clear();
                        last_key.extend_from_slice(iter.key().key_ref());
                    }
                    iter.next()?;
                    continue;
                }
                first_key_below_watermark = false;

                let key_ref = iter.key().key_ref();
                let mut matches_filter = false;
                for filter in &compaction_filters {
                    match filter {
                        CompactionFilter::Prefix(prefix) => {
                            if key_ref.starts_with(prefix) {
                                matches_filter = true;
                                break;
                            }
                        }
                    }
                }
                if matches_filter {
                    if !same_as_last_key {
                        last_key.clear();
                        last_key.extend_from_slice(iter.key().key_ref());
                    }
                    iter.next()?;
                    continue;
                }
            }

            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            builder.as_mut().unwrap().add(iter.key(), iter.value());

            if builder.as_ref().unwrap().estimated_size() >= self.options.target_sst_size
                && !same_as_last_key
            {
                let sst_id = self.next_sst_id();
                let builder = builder.take().unwrap();
                ssts.push(Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?));
            }

            if !same_as_last_key {
                last_key.clear();
                last_key.extend_from_slice(iter.key().key_ref());
            }

            iter.next()?;
        }

        if let Some(builder) = builder {
            let sst_id = self.next_sst_id();
            ssts.push(Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?));
        }

        Ok(ssts)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let l0_sstables = &snapshot.l0_sstables;
        let l1_sstables = &snapshot.levels[0].1;

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let tables = self.compact(&task)?;

        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();
            for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                new_state.sstables.remove(sst_id);
            }
            new_state
                .l0_sstables
                .retain(|sst_id| new_state.sstables.contains_key(sst_id));
            new_state.levels[0].1.clear();
            let mut output = Vec::with_capacity(tables.len());
            for table in tables {
                new_state.levels[0].1.push(table.sst_id());
                output.push(table.sst_id());
                new_state.sstables.insert(table.sst_id(), table);
            }

            self.manifest
                .add_record(&state_lock, ManifestRecord::Compaction(task, output))?;
            self.sync_dir()?;

            *state = Arc::new(new_state);
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };

        let tables = self.compact(&task)?;

        {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();

            let mut output = Vec::new();
            for table in &tables {
                snapshot.sstables.insert(table.sst_id(), table.clone());
                output.push(table.sst_id());
            }

            let (mut snapshot, sst_ids_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);

            for sst_id in &sst_ids_to_remove {
                snapshot.sstables.remove(sst_id);
            }

            self.manifest
                .add_record(&state_lock, ManifestRecord::Compaction(task, output))?;
            self.sync_dir()?;

            *self.state.write() = Arc::new(snapshot);
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {e}");
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if snapshot.imm_memtables.len() + 1 >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {e}");
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
