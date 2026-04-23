use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// Thread-safe progress tracker for sync operations.
/// Tracks blocks processed, events stored, and calculates throughput.
#[derive(Clone)]
pub struct ProgressTracker {
    inner: Arc<ProgressInner>,
}

struct ProgressInner {
    chain_id: u32,
    chain_name: String,
    start_block: u64,
    target_block: AtomicU64,
    current_block: AtomicU64,
    events_stored: AtomicU64,
    start_time: Instant,
}

impl ProgressTracker {
    /// Create a new progress tracker
    pub fn new(chain_id: u32, chain_name: &str, start_block: u64, target_block: u64) -> Self {
        Self {
            inner: Arc::new(ProgressInner {
                chain_id,
                chain_name: chain_name.to_string(),
                start_block,
                target_block: AtomicU64::new(target_block),
                current_block: AtomicU64::new(start_block),
                events_stored: AtomicU64::new(0),
                start_time: Instant::now(),
            }),
        }
    }

    /// Update the current block position
    pub fn set_current_block(&self, block: u64) {
        self.inner.current_block.store(block, Ordering::Relaxed);
    }

    /// Update the target block (e.g., when chain tip advances)
    pub fn set_target_block(&self, block: u64) {
        self.inner.target_block.store(block, Ordering::Relaxed);
    }

    /// Add to the total events stored count
    pub fn add_events(&self, count: u64) {
        self.inner
            .events_stored
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Get current block
    pub fn current_block(&self) -> u64 {
        self.inner.current_block.load(Ordering::Relaxed)
    }

    /// Get target block
    pub fn target_block(&self) -> u64 {
        self.inner.target_block.load(Ordering::Relaxed)
    }

    /// Get total events stored
    pub fn events_stored(&self) -> u64 {
        self.inner.events_stored.load(Ordering::Relaxed)
    }

    /// Calculate sync percentage (0.0 - 100.0)
    pub fn percentage(&self) -> f64 {
        let current = self.current_block();
        let target = self.target_block();
        let start = self.inner.start_block;

        if target <= start {
            return 100.0;
        }

        let processed = current.saturating_sub(start) as f64;
        let total = (target - start) as f64;

        (processed / total * 100.0).min(100.0)
    }

    /// Check if sync has caught up to target
    pub fn is_synced(&self) -> bool {
        self.current_block() >= self.target_block()
    }

    /// Calculate blocks per second throughput
    pub fn blocks_per_second(&self) -> f64 {
        let elapsed = self.inner.start_time.elapsed().as_secs_f64();
        if elapsed < 0.001 {
            return 0.0;
        }

        let processed = self.current_block().saturating_sub(self.inner.start_block) as f64;
        processed / elapsed
    }

    /// Log current progress
    pub fn log_progress(&self, mode: &str) {
        let current = self.current_block();
        let target = self.target_block();
        let percentage = self.percentage();
        let bps = self.blocks_per_second();
        let events = self.events_stored();

        info!(
            chain_id = self.inner.chain_id,
            chain = %self.inner.chain_name,
            mode,
            current_block = current,
            target_block = target,
            percentage = format!("{:.2}%", percentage),
            blocks_per_second = format!("{:.0}", bps),
            events_stored = events,
            "Sync progress"
        );
    }

    /// Estimate time remaining in seconds
    pub fn eta_seconds(&self) -> Option<f64> {
        let bps = self.blocks_per_second();
        if bps < 1.0 {
            return None;
        }

        let remaining = self.target_block().saturating_sub(self.current_block()) as f64;
        Some(remaining / bps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_tracker_basic() {
        let tracker = ProgressTracker::new(1, "ethereum", 100, 200);

        assert_eq!(tracker.current_block(), 100);
        assert_eq!(tracker.target_block(), 200);
        assert!(!tracker.is_synced());
    }

    #[test]
    fn test_progress_percentage() {
        let tracker = ProgressTracker::new(1, "ethereum", 0, 1000);

        tracker.set_current_block(500);
        assert!((tracker.percentage() - 50.0).abs() < 0.01);

        tracker.set_current_block(1000);
        assert!((tracker.percentage() - 100.0).abs() < 0.01);
        assert!(tracker.is_synced());
    }

    #[test]
    fn test_progress_events_counter() {
        let tracker = ProgressTracker::new(1, "ethereum", 0, 1000);

        tracker.add_events(100);
        tracker.add_events(200);
        assert_eq!(tracker.events_stored(), 300);
    }

    #[test]
    fn test_progress_target_update() {
        let tracker = ProgressTracker::new(1, "ethereum", 0, 1000);
        tracker.set_current_block(1000);
        assert!(tracker.is_synced());

        // Chain advances
        tracker.set_target_block(2000);
        assert!(!tracker.is_synced());
        assert!((tracker.percentage() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_progress_clone_shares_state() {
        let tracker1 = ProgressTracker::new(1, "ethereum", 0, 1000);
        let tracker2 = tracker1.clone();

        tracker1.set_current_block(500);
        assert_eq!(tracker2.current_block(), 500);

        tracker2.add_events(42);
        assert_eq!(tracker1.events_stored(), 42);
    }

    #[test]
    fn test_progress_same_start_target() {
        let tracker = ProgressTracker::new(1, "ethereum", 100, 100);
        assert_eq!(tracker.percentage(), 100.0);
        assert!(tracker.is_synced());
    }

    // === Additional progress tracker tests ===

    #[test]
    fn test_percentage_at_zero() {
        let tracker = ProgressTracker::new(1, "ethereum", 0, 1000);
        assert!((tracker.percentage() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_percentage_past_target_capped_at_100() {
        let tracker = ProgressTracker::new(1, "ethereum", 0, 1000);
        tracker.set_current_block(1500); // Past target
        assert!((tracker.percentage() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_percentage_target_less_than_start() {
        let tracker = ProgressTracker::new(1, "ethereum", 1000, 500);
        // target <= start, should return 100.0
        assert_eq!(tracker.percentage(), 100.0);
    }

    #[test]
    fn test_blocks_per_second_at_start() {
        // At start, elapsed ~0, should return 0.0
        let tracker = ProgressTracker::new(1, "ethereum", 100, 200);
        let bps = tracker.blocks_per_second();
        // This will be 0 since we haven't advanced from start_block
        assert!((bps - 0.0).abs() < 0.1);
    }

    #[test]
    fn test_eta_returns_none_when_slow() {
        // blocks_per_second < 1.0 should return None
        let tracker = ProgressTracker::new(1, "ethereum", 100, 200);
        // Haven't processed anything, bps should be < 1.0
        assert!(tracker.eta_seconds().is_none());
    }

    #[test]
    fn test_is_synced_exact_target() {
        let tracker = ProgressTracker::new(1, "ethereum", 0, 1000);
        tracker.set_current_block(1000);
        assert!(tracker.is_synced());
    }

    #[test]
    fn test_is_synced_past_target() {
        let tracker = ProgressTracker::new(1, "ethereum", 0, 1000);
        tracker.set_current_block(1001);
        assert!(tracker.is_synced());
    }

    #[test]
    fn test_events_stored_atomic() {
        let tracker = ProgressTracker::new(1, "ethereum", 0, 1000);
        tracker.add_events(0);
        assert_eq!(tracker.events_stored(), 0);
        tracker.add_events(1);
        assert_eq!(tracker.events_stored(), 1);
        tracker.add_events(u64::MAX - 1);
        // Wrapping add
        assert_eq!(tracker.events_stored(), u64::MAX);
    }
}
