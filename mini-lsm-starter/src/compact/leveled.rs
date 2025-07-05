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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let first_key = sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].first_key())
            .min()
            .unwrap();
        let last_key = sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].last_key())
            .max()
            .unwrap();

        let mut result_ssts = Vec::new();
        for lower_sst_id in &snapshot.levels[in_level - 1].1 {
            let lower_table = &snapshot.sstables[lower_sst_id];

            if !(lower_table.last_key() < first_key || lower_table.first_key() >= last_key) {
                result_ssts.push(*lower_sst_id);
            }
        }

        result_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut target_level_sizes = vec![0; self.options.max_levels];
        let mut real_level_sizes = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            let (_, sst_ids) = &snapshot.levels[i];
            let size: u64 = sst_ids
                .iter()
                .map(|sst_id| snapshot.sstables[sst_id].table_size())
                .sum();
            real_level_sizes.push(size);
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        target_level_sizes[self.options.max_levels - 1] = real_level_sizes
            [self.options.max_levels - 1]
            .max(self.options.base_level_size_mb as u64 * 1024 * 1024);

        for i in (0..self.options.max_levels - 1).rev() {
            let next_level_size = target_level_sizes[i + 1] as u64;
            let this_level_size = next_level_size / self.options.level_size_multiplier as u64;
            if next_level_size > base_level_size_bytes as u64 {
                target_level_sizes[i] = this_level_size;
            }
            if target_level_sizes[i] > 0 {
                base_level = i + 1;
            }
        }

        // l0 check
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SSTs to base level {base_level}");
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        // other level check
        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for level in 0..self.options.max_levels {
            let ratio = real_level_sizes[level] as f64 / target_level_sizes[level] as f64;
            if ratio > 1.0 {
                priorities.push((ratio, level + 1));
            }
        }
        priorities.sort_by(|a, b| a.0.total_cmp(&b.0).reverse());

        if let Some((ratio, level)) = priorities.first() {
            let lower_level = level + 1;
            let selected_sst = *snapshot.levels[level - 1].1.iter().min().unwrap();
            println!(
                "flush L{level} {selected_sst} SST to L{lower_level} due to ratio {ratio} > 1.0"
            );
            return Some(LeveledCompactionTask {
                upper_level: Some(*level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[selected_sst],
                    lower_level,
                ),
                is_lower_level_bottom_level: lower_level == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let LeveledCompactionTask {
            upper_level,
            upper_level_sst_ids,
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level,
        } = task;
        let mut state = snapshot.clone();

        let mut upper_level_sst_ids_to_remove = upper_level_sst_ids.iter().collect::<HashSet<_>>();
        if let Some(upper_level) = upper_level {
            state.levels[*upper_level - 1]
                .1
                .retain(|sst_id| !upper_level_sst_ids_to_remove.remove(sst_id));
        } else {
            state
                .l0_sstables
                .retain(|sst_id| !upper_level_sst_ids_to_remove.remove(sst_id));
        }
        assert!(upper_level_sst_ids_to_remove.is_empty());

        let mut lower_level_sst_ids_to_remove = lower_level_sst_ids.iter().collect::<HashSet<_>>();
        state.levels[*lower_level - 1]
            .1
            .retain(|sst_id| !lower_level_sst_ids_to_remove.remove(sst_id));
        assert!(lower_level_sst_ids_to_remove.is_empty());

        state.levels[*lower_level - 1].1.extend_from_slice(output);
        state.levels[*lower_level - 1]
            .1
            .sort_by_key(|sst_id| snapshot.sstables[sst_id].first_key());

        let tables_to_remove = upper_level_sst_ids
            .iter()
            .chain(lower_level_sst_ids.iter())
            .cloned()
            .collect();
        (state, tables_to_remove)
    }
}
