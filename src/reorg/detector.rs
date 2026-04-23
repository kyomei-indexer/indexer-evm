use crate::sources::BlockSource;
use anyhow::{Context, Result};
use std::collections::VecDeque;
use tracing::{info, warn};

/// Detects chain reorganizations by tracking recent block hashes
/// and validating parent hash chains.
///
/// Ported from packages/core/src/services/ReorgDetector.ts
pub struct ReorgDetector {
    /// Rolling window of recent (block_number, block_hash) pairs
    known_blocks: VecDeque<(u64, String)>,
    /// Maximum number of blocks to keep in the rolling window
    max_depth: usize,
    /// Number of finality confirmations for this chain
    finality_blocks: u64,
}

/// Result of a reorg check
#[derive(Debug, Clone)]
pub enum ReorgCheckResult {
    /// No reorg detected, chain is consistent
    Ok,
    /// Reorg detected: common ancestor block and depth
    Reorg {
        common_ancestor_block: u64,
        depth: u64,
        reorged_blocks: Vec<u64>,
    },
}

impl ReorgDetector {
    pub fn new(max_depth: usize, finality_blocks: u64) -> Self {
        Self {
            known_blocks: VecDeque::with_capacity(max_depth),
            max_depth,
            finality_blocks,
        }
    }

    /// Record a new block hash (called after successfully processing a block)
    pub fn record_block(&mut self, block_number: u64, block_hash: &str) {
        if self.max_depth == 0 {
            return;
        }
        // Remove blocks older than max_depth
        while self.known_blocks.len() >= self.max_depth {
            self.known_blocks.pop_front();
        }
        self.known_blocks
            .push_back((block_number, block_hash.to_string()));
    }

    /// Validate a new block against the known chain.
    /// Returns Ok if the block's parent hash matches our last known block hash.
    pub fn validate_block(
        &self,
        block_number: u64,
        _block_hash: &str,
        parent_hash: &str,
    ) -> ReorgCheckResult {
        // If we have no history, we can't detect reorgs
        if self.known_blocks.is_empty() {
            return ReorgCheckResult::Ok;
        }

        // Find the expected parent block
        let expected_parent_number = block_number.saturating_sub(1);
        let known_parent = self
            .known_blocks
            .iter()
            .find(|(num, _)| *num == expected_parent_number);

        match known_parent {
            Some((_, known_hash)) => {
                if known_hash == parent_hash {
                    // Chain is consistent
                    ReorgCheckResult::Ok
                } else {
                    // Parent hash mismatch — reorg detected!
                    // Walk back through known blocks to find the common ancestor
                    let (ancestor, depth, reorged) = self.find_common_ancestor(block_number);
                    warn!(
                        block_number,
                        depth,
                        common_ancestor = ancestor,
                        "Reorg detected"
                    );
                    ReorgCheckResult::Reorg {
                        common_ancestor_block: ancestor,
                        depth,
                        reorged_blocks: reorged,
                    }
                }
            }
            None => {
                // We don't have this parent block in our window.
                // This could be a gap or a very deep reorg.
                // For safety, treat it as OK if we're building from a new start point.
                ReorgCheckResult::Ok
            }
        }
    }

    /// Find the common ancestor by querying the block source.
    /// This is the async version used when we need to walk back further.
    pub async fn find_common_ancestor_with_source(
        &self,
        block_number: u64,
        source: &dyn BlockSource,
    ) -> Result<ReorgCheckResult> {
        let mut depth = 0u64;
        let mut check_block = block_number;
        let mut reorged_blocks = Vec::new();

        while depth < self.max_depth as u64 {
            let our_hash = self
                .known_blocks
                .iter()
                .find(|(num, _)| *num == check_block)
                .map(|(_, hash)| hash.as_str());

            if our_hash.is_none() {
                // We've gone past our known window, assume the common ancestor is here
                break;
            }

            let chain_hash = source
                .get_block_hash(check_block)
                .await
                .context("Failed to get block hash for reorg detection")?;

            match chain_hash {
                Some(ref hash) if hash == our_hash.unwrap() => {
                    // Found the common ancestor
                    if depth > 0 {
                        info!(
                            common_ancestor = check_block,
                            depth,
                            "Found common ancestor after reorg"
                        );
                        return Ok(ReorgCheckResult::Reorg {
                            common_ancestor_block: check_block,
                            depth,
                            reorged_blocks,
                        });
                    }
                    return Ok(ReorgCheckResult::Ok);
                }
                _ => {
                    reorged_blocks.push(check_block);
                    depth += 1;
                    if check_block == 0 {
                        break;
                    }
                    check_block -= 1;
                }
            }
        }

        if depth > 0 {
            Ok(ReorgCheckResult::Reorg {
                common_ancestor_block: check_block,
                depth,
                reorged_blocks,
            })
        } else {
            Ok(ReorgCheckResult::Ok)
        }
    }

    /// Remove known blocks that are at or after the given block number
    /// (used after handling a reorg to clean up invalidated state)
    pub fn invalidate_from(&mut self, from_block: u64) {
        self.known_blocks.retain(|(num, _)| *num < from_block);
    }

    /// Get the latest known block number
    pub fn latest_known_block(&self) -> Option<u64> {
        self.known_blocks.back().map(|(num, _)| *num)
    }

    /// Check if a block number is considered final (past finality depth)
    pub fn is_final(&self, block_number: u64, current_block: u64) -> bool {
        current_block.saturating_sub(block_number) >= self.finality_blocks
    }

    /// Private helper to find common ancestor from local state only
    fn find_common_ancestor(&self, from_block: u64) -> (u64, u64, Vec<u64>) {
        let mut depth = 0u64;
        let mut reorged = Vec::new();

        // Walk backwards through known blocks
        for (num, _) in self.known_blocks.iter().rev() {
            if *num >= from_block {
                continue;
            }
            // We mark everything above this as reorged
            depth += 1;
            reorged.push(*num);
        }

        // The common ancestor is one before the first reorged block,
        // or the last block we checked
        let ancestor = if let Some(&first_reorged) = reorged.last() {
            first_reorged.saturating_sub(1)
        } else {
            from_block.saturating_sub(1)
        };

        (ancestor, depth, reorged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_validate_consistent_chain() {
        let mut detector = ReorgDetector::new(100, 65);

        // Record blocks 1-5
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Block 6 with correct parent hash should be OK
        let result = detector.validate_block(6, "hash_6", "hash_5");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_detect_reorg_parent_mismatch() {
        let mut detector = ReorgDetector::new(100, 65);

        // Record blocks 1-5
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Block 6 with wrong parent hash should detect reorg
        let result = detector.validate_block(6, "hash_6_new", "hash_5_wrong");
        assert!(matches!(result, ReorgCheckResult::Reorg { .. }));
    }

    #[test]
    fn test_empty_detector_returns_ok() {
        let detector = ReorgDetector::new(100, 65);
        let result = detector.validate_block(1, "hash_1", "hash_0");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_invalidate_from() {
        let mut detector = ReorgDetector::new(100, 65);

        for i in 1..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        assert_eq!(detector.latest_known_block(), Some(10));

        detector.invalidate_from(8);
        assert_eq!(detector.latest_known_block(), Some(7));
    }

    #[test]
    fn test_max_depth_rolling_window() {
        let mut detector = ReorgDetector::new(5, 65);

        // Record 10 blocks — only last 5 should be kept
        for i in 1..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Block 6 should have been evicted
        assert!(detector
            .known_blocks
            .iter()
            .find(|(num, _)| *num == 6)
            .is_some());

        // Block 5 should have been evicted (only 5 slots)
        assert!(detector
            .known_blocks
            .iter()
            .find(|(num, _)| *num == 5)
            .is_none());
    }

    #[test]
    fn test_finality_check() {
        let detector = ReorgDetector::new(100, 65);

        assert!(!detector.is_final(100, 150)); // 50 < 65
        assert!(detector.is_final(100, 165)); // 65 >= 65
        assert!(detector.is_final(100, 200)); // 100 >= 65
    }

    #[test]
    fn test_gap_in_known_blocks_returns_ok() {
        let mut detector = ReorgDetector::new(100, 65);

        // Record only blocks 1-3
        for i in 1..=3 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Block 10 has parent that we don't know about — should return OK (gap)
        let result = detector.validate_block(10, "hash_10", "hash_9");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    // === Additional reorg detector edge case tests ===

    #[test]
    fn test_invalidate_from_clears_all() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }
        detector.invalidate_from(1); // Invalidate everything from block 1
        assert!(detector.latest_known_block().is_none());
        assert!(detector.known_blocks.is_empty());
    }

    #[test]
    fn test_invalidate_from_beyond_range() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }
        detector.invalidate_from(100); // Nothing to invalidate
        assert_eq!(detector.latest_known_block(), Some(5));
        assert_eq!(detector.known_blocks.len(), 5);
    }

    #[test]
    fn test_latest_known_block_empty() {
        let detector = ReorgDetector::new(100, 65);
        assert!(detector.latest_known_block().is_none());
    }

    #[test]
    fn test_latest_known_block_single() {
        let mut detector = ReorgDetector::new(100, 65);
        detector.record_block(42, "hash_42");
        assert_eq!(detector.latest_known_block(), Some(42));
    }

    #[test]
    fn test_is_final_at_exact_boundary() {
        let detector = ReorgDetector::new(100, 65);
        // current=165, block=100: difference=65, which is >= 65
        assert!(detector.is_final(100, 165));
        // current=164, block=100: difference=64, which is < 65
        assert!(!detector.is_final(100, 164));
    }

    #[test]
    fn test_is_final_current_less_than_block() {
        let detector = ReorgDetector::new(100, 65);
        // Saturating subtraction: 50 - 100 = 0, which is < 65
        assert!(!detector.is_final(100, 50));
    }

    #[test]
    fn test_is_final_same_block() {
        let detector = ReorgDetector::new(100, 65);
        assert!(!detector.is_final(100, 100));
    }

    #[test]
    fn test_max_depth_1_only_keeps_latest() {
        let mut detector = ReorgDetector::new(1, 65);
        detector.record_block(1, "hash_1");
        detector.record_block(2, "hash_2");
        assert_eq!(detector.known_blocks.len(), 1);
        assert_eq!(detector.latest_known_block(), Some(2));
    }

    #[test]
    fn test_validate_consecutive_blocks() {
        let mut detector = ReorgDetector::new(100, 65);

        // Simulate a normal chain of 10 blocks
        for i in 1..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Each next block validates correctly
        let result = detector.validate_block(11, "hash_11", "hash_10");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_reorg_returns_depth_and_blocks() {
        let mut detector = ReorgDetector::new(100, 65);

        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Block 6 with wrong parent hash
        let result = detector.validate_block(6, "hash_6_new", "hash_5_wrong");

        match result {
            ReorgCheckResult::Reorg { depth, .. } => {
                assert!(depth > 0);
            }
            ReorgCheckResult::Ok => panic!("Should have detected reorg"),
        }
    }

    #[test]
    fn test_record_then_invalidate_then_record() {
        let mut detector = ReorgDetector::new(100, 65);

        // Record blocks 1-5
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Invalidate from block 4
        detector.invalidate_from(4);
        assert_eq!(detector.latest_known_block(), Some(3));

        // Record new blocks 4-6 (fork)
        for i in 4..=6 {
            detector.record_block(i, &format!("fork_hash_{}", i));
        }
        assert_eq!(detector.latest_known_block(), Some(6));
        assert_eq!(detector.known_blocks.len(), 6); // 1,2,3 + 4,5,6
    }

    // ========================================================================
    // Deep reorg detection edge case tests
    // ========================================================================

    #[test]
    fn test_reorg_at_block_1_parent_mismatch_on_genesis_child() {
        let mut detector = ReorgDetector::new(100, 65);
        // Only block 0 known (genesis)
        detector.record_block(0, "genesis_hash");

        // Block 1 claims a different parent than genesis
        let result = detector.validate_block(1, "hash_1", "wrong_genesis");
        assert!(matches!(result, ReorgCheckResult::Reorg { .. }));
    }

    #[test]
    fn test_reorg_at_block_1_correct_parent() {
        let mut detector = ReorgDetector::new(100, 65);
        detector.record_block(0, "genesis_hash");

        let result = detector.validate_block(1, "hash_1", "genesis_hash");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_deep_reorg_beyond_window_goes_undetected() {
        // This is a critical edge case: if the reorg depth exceeds the rolling
        // window, the parent won't be found and we return Ok (silent miss).
        let mut detector = ReorgDetector::new(5, 65);

        // Record blocks 6-10 (window of 5)
        for i in 6..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Block 5's parent (block 4) is outside our window -> returns Ok
        // This means a reorg at block 5 would go UNDETECTED
        let result = detector.validate_block(5, "hash_5_new", "hash_4_wrong");
        assert!(
            matches!(result, ReorgCheckResult::Ok),
            "Deep reorg outside window should return Ok (undetected)"
        );
    }

    #[test]
    fn test_reorg_exactly_at_window_boundary() {
        let mut detector = ReorgDetector::new(5, 65);

        // Record blocks 1-5
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Block 6's parent (block 5) IS in the window
        let result = detector.validate_block(6, "hash_6", "wrong_hash_5");
        assert!(matches!(result, ReorgCheckResult::Reorg { .. }));
    }

    #[test]
    fn test_consecutive_reorgs_invalidate_and_rebuild() {
        let mut detector = ReorgDetector::new(100, 65);

        // Build chain: 1..10
        for i in 1..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // First reorg at block 8
        let result = detector.validate_block(11, "new_11", "wrong_10");
        assert!(matches!(result, ReorgCheckResult::Reorg { .. }));

        // Handle reorg: invalidate from block 8
        detector.invalidate_from(8);
        assert_eq!(detector.latest_known_block(), Some(7));

        // Rebuild chain 8-10 on the new fork
        for i in 8..=10 {
            detector.record_block(i, &format!("fork1_hash_{}", i));
        }

        // Second reorg at block 9
        let result = detector.validate_block(11, "new_11_v2", "wrong_fork1_10");
        assert!(matches!(result, ReorgCheckResult::Reorg { .. }));

        // Handle second reorg
        detector.invalidate_from(9);
        assert_eq!(detector.latest_known_block(), Some(8));

        // Rebuild again
        for i in 9..=10 {
            detector.record_block(i, &format!("fork2_hash_{}", i));
        }

        // Now chain is consistent
        let result = detector.validate_block(11, "hash_11", "fork2_hash_10");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_validate_block_0_with_empty_detector() {
        let detector = ReorgDetector::new(100, 65);
        // Block 0 (genesis) with no known blocks
        let result = detector.validate_block(0, "genesis", "0x0");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_validate_block_0_saturating_sub_parent() {
        let mut detector = ReorgDetector::new(100, 65);
        // block_number.saturating_sub(1) for block 0 = 0
        // If we have block 0 recorded, it tries to match parent of block 0
        // which is block 0 itself (saturating_sub), this is an edge case
        detector.record_block(0, "genesis_hash");
        // Block 0's "parent" via saturating_sub is block 0 itself
        let result = detector.validate_block(0, "genesis_v2", "genesis_hash");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_non_sequential_block_recording() {
        let mut detector = ReorgDetector::new(100, 65);

        // Record blocks out of order (e.g., from parallel workers)
        detector.record_block(5, "hash_5");
        detector.record_block(3, "hash_3");
        detector.record_block(10, "hash_10");
        detector.record_block(7, "hash_7");

        // Should still find block 10 for validating block 11
        let result = detector.validate_block(11, "hash_11", "hash_10");
        assert!(matches!(result, ReorgCheckResult::Ok));

        // But gap: block 6 not recorded, so validating block 7 returns Ok (gap)
        let result = detector.validate_block(7, "hash_7_new", "hash_6_unknown");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_duplicate_block_recording() {
        let mut detector = ReorgDetector::new(100, 65);

        // Record the same block twice (shouldn't break anything)
        detector.record_block(5, "hash_5");
        detector.record_block(5, "hash_5_v2");

        // Both entries exist in the deque
        let count = detector
            .known_blocks
            .iter()
            .filter(|(num, _)| *num == 5)
            .count();
        assert_eq!(count, 2);

        // validate_block uses iter().find() which returns the first match
        // So it will use "hash_5", not "hash_5_v2"
        let result = detector.validate_block(6, "hash_6", "hash_5");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_overwrite_block_after_invalidate() {
        let mut detector = ReorgDetector::new(100, 65);

        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Reorg: invalidate block 5 and record new hash
        detector.invalidate_from(5);
        detector.record_block(5, "new_hash_5");

        // Block 6 should match against new_hash_5
        let result = detector.validate_block(6, "hash_6", "new_hash_5");
        assert!(matches!(result, ReorgCheckResult::Ok));

        // Old hash should NOT match
        let result = detector.validate_block(6, "hash_6", "hash_5");
        assert!(matches!(result, ReorgCheckResult::Reorg { .. }));
    }

    #[test]
    fn test_invalidate_from_0() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 0..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }
        detector.invalidate_from(0);
        assert!(detector.known_blocks.is_empty());
    }

    #[test]
    fn test_finality_blocks_0_means_always_final() {
        let detector = ReorgDetector::new(100, 0);
        // With finality_blocks=0, even the current block is "final"
        assert!(detector.is_final(100, 100));
        assert!(detector.is_final(0, 0));
    }

    #[test]
    fn test_finality_u64_max_overflow_protection() {
        let detector = ReorgDetector::new(100, u64::MAX);
        // current_block.saturating_sub(block_number) can never reach u64::MAX
        assert!(!detector.is_final(0, u64::MAX - 1));
        // But u64::MAX - 0 = u64::MAX, which equals finality_blocks
        assert!(detector.is_final(0, u64::MAX));
    }

    #[test]
    fn test_max_depth_0_is_noop() {
        // max_depth=0 means the detector cannot record any blocks
        let mut detector = ReorgDetector::new(0, 65);
        detector.record_block(1, "hash_1");
        detector.record_block(2, "hash_2");
        assert!(detector.known_blocks.is_empty());
        assert!(detector.latest_known_block().is_none());
        // Validation with no known blocks always returns Ok
        let result = detector.validate_block(3, "hash_3", "hash_2");
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[test]
    fn test_max_depth_1_keeps_only_latest() {
        let mut detector = ReorgDetector::new(1, 65);
        detector.record_block(1, "hash_1");
        detector.record_block(2, "hash_2");
        detector.record_block(3, "hash_3");
        assert_eq!(detector.known_blocks.len(), 1);
        assert_eq!(detector.latest_known_block(), Some(3));
    }

    #[test]
    fn test_find_common_ancestor_single_block_reorg() {
        let mut detector = ReorgDetector::new(100, 65);

        // Record blocks 1-10
        for i in 1..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Block 11 has wrong parent -> reorg
        let result = detector.validate_block(11, "hash_11_new", "wrong_hash_10");
        match result {
            ReorgCheckResult::Reorg {
                depth,
                reorged_blocks,
                ..
            } => {
                assert!(depth > 0);
                assert!(!reorged_blocks.is_empty());
            }
            ReorgCheckResult::Ok => panic!("Expected reorg detection"),
        }
    }

    #[test]
    fn test_rapid_invalidate_record_cycle_stress() {
        let mut detector = ReorgDetector::new(10, 65);

        // Simulate rapid reorg-recovery cycles
        for cycle in 0..50 {
            let base = cycle * 5;
            for i in 0..5 {
                detector.record_block(base + i, &format!("hash_{}_{}", cycle, i));
            }
            // Invalidate the last 2 blocks
            detector.invalidate_from(base + 3);
        }

        // Detector should still be functional
        assert!(detector.latest_known_block().is_some());
        assert!(detector.known_blocks.len() <= 10);
    }

    #[test]
    fn test_validate_with_very_large_block_numbers() {
        let mut detector = ReorgDetector::new(100, 65);
        let large = u64::MAX - 10;

        detector.record_block(large, "hash_large");
        detector.record_block(large + 1, "hash_large_plus_1");

        // Validate the next block
        let result = detector.validate_block(large + 2, "hash", "hash_large_plus_1");
        assert!(matches!(result, ReorgCheckResult::Ok));

        // Wrong parent
        let result = detector.validate_block(large + 2, "hash", "wrong");
        assert!(matches!(result, ReorgCheckResult::Reorg { .. }));
    }

    // ========================================================================
    // Async reorg detection with mock BlockSource
    // ========================================================================

    use crate::sources::BlockSource;
    use crate::types::{BlockRange, BlockWithLogs, LogFilter};
    use async_trait::async_trait;
    use std::collections::HashMap;

    /// Mock block source for testing find_common_ancestor_with_source
    struct MockBlockSource {
        hashes: HashMap<u64, String>,
    }

    impl MockBlockSource {
        fn new(hashes: Vec<(u64, &str)>) -> Self {
            Self {
                hashes: hashes
                    .into_iter()
                    .map(|(n, h)| (n, h.to_string()))
                    .collect(),
            }
        }
    }

    #[async_trait]
    impl BlockSource for MockBlockSource {
        async fn get_blocks(
            &self,
            _range: BlockRange,
            _filter: &LogFilter,
        ) -> anyhow::Result<Vec<BlockWithLogs>> {
            Ok(vec![])
        }

        async fn get_latest_block_number(&self) -> anyhow::Result<u64> {
            Ok(0)
        }

        async fn get_block_hash(&self, block_number: u64) -> anyhow::Result<Option<String>> {
            Ok(self.hashes.get(&block_number).cloned())
        }

        fn source_type(&self) -> &'static str {
            "mock"
        }
    }

    #[tokio::test]
    async fn test_find_ancestor_with_source_no_reorg() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Chain source agrees with our hashes
        let source = MockBlockSource::new(vec![
            (1, "hash_1"),
            (2, "hash_2"),
            (3, "hash_3"),
            (4, "hash_4"),
            (5, "hash_5"),
        ]);

        let result = detector
            .find_common_ancestor_with_source(5, &source)
            .await
            .unwrap();
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[tokio::test]
    async fn test_find_ancestor_with_source_single_block_reorg() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Chain forked at block 5 (different hash), blocks 1-4 match
        let source = MockBlockSource::new(vec![
            (1, "hash_1"),
            (2, "hash_2"),
            (3, "hash_3"),
            (4, "hash_4"),
            (5, "hash_5_REORGED"),
        ]);

        let result = detector
            .find_common_ancestor_with_source(5, &source)
            .await
            .unwrap();
        match result {
            ReorgCheckResult::Reorg {
                common_ancestor_block,
                depth,
                reorged_blocks,
            } => {
                assert_eq!(common_ancestor_block, 4);
                assert_eq!(depth, 1);
                assert_eq!(reorged_blocks, vec![5]);
            }
            ReorgCheckResult::Ok => panic!("Expected reorg"),
        }
    }

    #[tokio::test]
    async fn test_find_ancestor_with_source_multi_block_reorg() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 1..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Chain diverged at block 8: blocks 8,9,10 have different hashes
        let source = MockBlockSource::new(vec![
            (5, "hash_5"),
            (6, "hash_6"),
            (7, "hash_7"),
            (8, "REORGED_8"),
            (9, "REORGED_9"),
            (10, "REORGED_10"),
        ]);

        let result = detector
            .find_common_ancestor_with_source(10, &source)
            .await
            .unwrap();
        match result {
            ReorgCheckResult::Reorg {
                common_ancestor_block,
                depth,
                reorged_blocks,
            } => {
                assert_eq!(common_ancestor_block, 7);
                assert_eq!(depth, 3);
                assert_eq!(reorged_blocks, vec![10, 9, 8]);
            }
            ReorgCheckResult::Ok => panic!("Expected reorg"),
        }
    }

    #[tokio::test]
    async fn test_find_ancestor_with_source_all_blocks_reorged() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Every known block has a different hash on chain
        let source = MockBlockSource::new(vec![
            (1, "DIFFERENT_1"),
            (2, "DIFFERENT_2"),
            (3, "DIFFERENT_3"),
            (4, "DIFFERENT_4"),
            (5, "DIFFERENT_5"),
        ]);

        let result = detector
            .find_common_ancestor_with_source(5, &source)
            .await
            .unwrap();
        match result {
            ReorgCheckResult::Reorg { depth, .. } => {
                assert_eq!(depth, 5);
            }
            ReorgCheckResult::Ok => panic!("Expected reorg"),
        }
    }

    #[tokio::test]
    async fn test_find_ancestor_with_source_chain_returns_none() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 1..=5 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // Chain source returns None for all blocks (block not found)
        let source = MockBlockSource::new(vec![]);

        let result = detector
            .find_common_ancestor_with_source(5, &source)
            .await
            .unwrap();
        // None hashes don't match, so treated as reorg
        match result {
            ReorgCheckResult::Reorg { depth, .. } => {
                assert_eq!(depth, 5);
            }
            ReorgCheckResult::Ok => panic!("Expected reorg when chain returns None"),
        }
    }

    #[tokio::test]
    async fn test_find_ancestor_with_source_empty_detector() {
        let detector = ReorgDetector::new(100, 65);
        let source = MockBlockSource::new(vec![(5, "hash_5")]);

        // No known blocks -> exits immediately -> Ok
        let result = detector
            .find_common_ancestor_with_source(5, &source)
            .await
            .unwrap();
        assert!(matches!(result, ReorgCheckResult::Ok));
    }

    #[tokio::test]
    async fn test_find_ancestor_with_source_walks_to_block_0() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 0..=3 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // All blocks reorged, walk all the way to block 0
        let source = MockBlockSource::new(vec![
            (0, "DIFFERENT_0"),
            (1, "DIFFERENT_1"),
            (2, "DIFFERENT_2"),
            (3, "DIFFERENT_3"),
        ]);

        let result = detector
            .find_common_ancestor_with_source(3, &source)
            .await
            .unwrap();
        match result {
            ReorgCheckResult::Reorg {
                common_ancestor_block,
                depth,
                reorged_blocks,
            } => {
                // Walked from 3 down to 0, all different
                assert_eq!(depth, 4);
                assert_eq!(reorged_blocks, vec![3, 2, 1, 0]);
                // check_block ends at 0 after the loop sees check_block==0 and breaks
                assert_eq!(common_ancestor_block, 0);
            }
            ReorgCheckResult::Ok => panic!("Expected reorg"),
        }
    }

    #[tokio::test]
    async fn test_find_ancestor_limited_by_max_depth() {
        // max_depth = 3, but we have 10 blocks known
        let mut detector = ReorgDetector::new(3, 65);
        // Only last 3 will be kept due to max_depth in record_block
        for i in 1..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // All chain hashes are different
        let source = MockBlockSource::new(vec![
            (8, "DIFF_8"),
            (9, "DIFF_9"),
            (10, "DIFF_10"),
        ]);

        let result = detector
            .find_common_ancestor_with_source(10, &source)
            .await
            .unwrap();
        match result {
            ReorgCheckResult::Reorg { depth, .. } => {
                // Should walk back max 3 blocks (limited by max_depth)
                assert!(depth <= 3);
            }
            ReorgCheckResult::Ok => panic!("Expected reorg"),
        }
    }

    #[tokio::test]
    async fn test_find_ancestor_reorg_then_consistent_recovery() {
        let mut detector = ReorgDetector::new(100, 65);
        for i in 1..=10 {
            detector.record_block(i, &format!("hash_{}", i));
        }

        // First: detect reorg
        let source = MockBlockSource::new(vec![
            (7, "hash_7"),
            (8, "REORGED_8"),
            (9, "REORGED_9"),
            (10, "REORGED_10"),
        ]);

        let result = detector
            .find_common_ancestor_with_source(10, &source)
            .await
            .unwrap();
        match &result {
            ReorgCheckResult::Reorg {
                common_ancestor_block,
                ..
            } => {
                assert_eq!(*common_ancestor_block, 7);
            }
            _ => panic!("Expected reorg"),
        }

        // Invalidate reorged blocks
        detector.invalidate_from(8);

        // Re-record the correct chain
        detector.record_block(8, "REORGED_8");
        detector.record_block(9, "REORGED_9");
        detector.record_block(10, "REORGED_10");

        // Now validation should pass
        let source2 = MockBlockSource::new(vec![
            (10, "REORGED_10"),
        ]);

        let result = detector
            .find_common_ancestor_with_source(10, &source2)
            .await
            .unwrap();
        assert!(matches!(result, ReorgCheckResult::Ok));
    }
}
