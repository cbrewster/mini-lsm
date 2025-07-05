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
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Space Amplification Ratio
        let all_levels_size: usize = snapshot.levels.iter().map(|(_, ids)| Vec::len(ids)).sum();
        let last_levels_size = snapshot.levels.last().unwrap().1.len();
        let space_amplification_ratio =
            (all_levels_size - last_levels_size) as f64 / last_levels_size as f64;
        if space_amplification_ratio >= self.options.max_size_amplification_percent as f64 / 100.0 {
            println!(
                "Trigging compaction due to space amplication ratio {space_amplification_ratio}"
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Tier Size Ratio
        let mut running_size = snapshot.levels[0].1.len();
        for (i, (_, level_tables)) in snapshot.levels.iter().enumerate().skip(1) {
            let size_ratio = level_tables.len() as f64 / running_size as f64;
            if i >= self.options.min_merge_width
                && size_ratio > (100.0 + self.options.size_ratio as f64) * 0.01
            {
                println!(
                    "Trigging compaction due to tier size ratio {size_ratio} > {}",
                    (100.0 + self.options.size_ratio as f64) * 0.01
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[0..i].to_vec(),
                    // bottom tier is never included here.
                    bottom_tier_included: false,
                });
            }
            running_size += level_tables.len();
        }

        // Reduce sorted runs
        let width = match self.options.max_merge_width {
            Some(max_merge_width) => max_merge_width.min(snapshot.levels.len()),
            None => snapshot.levels.len(),
        };
        Some(TieredCompactionTask {
            tiers: snapshot.levels[0..width].to_vec(),
            bottom_tier_included: width == snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut state = snapshot.clone();
        let insert_idx = snapshot
            .levels
            .iter()
            .map(|(id, _)| *id)
            .position(|id| id == task.tiers[0].0)
            .unwrap();

        let mut tiers_to_remove = task.tiers.iter().map(|(id, _)| id).collect::<HashSet<_>>();
        state.levels.retain(|(id, _)| !tiers_to_remove.remove(id));
        assert!(
            tiers_to_remove.is_empty(),
            "expected all compacted tiers to be present in snapshot"
        );

        state.levels.insert(insert_idx, (output[0], output.into()));

        let files_to_remove = task
            .tiers
            .iter()
            .flat_map(|(_, sst_ids)| sst_ids.iter().cloned())
            .collect();

        (state, files_to_remove)
    }
}
