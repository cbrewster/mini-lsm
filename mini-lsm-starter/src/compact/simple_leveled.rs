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

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!(
                "compaction triggered at level 0 and 1 with l0 file number trigger {}",
                self.options.level0_file_num_compaction_trigger
            );
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }

        for upper_level in 1..self.options.max_levels {
            let lower_level = upper_level + 1;
            let upper_file_count = snapshot.levels[upper_level - 1].1.len();
            let lower_file_count = snapshot.levels[lower_level - 1].1.len();
            let ratio = lower_file_count as f64 / upper_file_count as f64;
            if ratio < self.options.size_ratio_percent as f64 / 100.0 {
                println!(
                    "compaction triggered at level {upper_level} and {lower_level} with size ratio {ratio}",
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: snapshot.levels[upper_level - 1].1.clone(),
                    lower_level,
                    lower_level_sst_ids: snapshot.levels[lower_level - 1].1.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let SimpleLeveledCompactionTask {
            upper_level,
            upper_level_sst_ids,
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level,
        } = task;
        let mut state = snapshot.clone();

        let mut tables_to_remove = upper_level_sst_ids.clone();
        if let Some(upper_level) = upper_level {
            assert_eq!(upper_level_sst_ids, &state.levels[*upper_level - 1].1);
            state.levels[*upper_level - 1].1.clear();
        } else {
            let mut upper_level_sst_ids_set = upper_level_sst_ids.iter().collect::<HashSet<_>>();
            state
                .l0_sstables
                .retain(|sst_id| !upper_level_sst_ids_set.remove(sst_id));
            assert!(upper_level_sst_ids_set.is_empty());
        }

        tables_to_remove.extend_from_slice(&state.levels[*lower_level - 1].1);
        state.levels[*lower_level - 1].1 = output.to_vec();

        (state, tables_to_remove)
    }
}
