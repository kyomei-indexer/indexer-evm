use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

/// Adaptive concurrency limiter that grows on success and shrinks on rate limits.
/// Shared across all workers hitting the same RPC to prevent thundering herd.
pub struct AdaptiveConcurrency {
    semaphore: Arc<Semaphore>,
    max_permits: u32,
    current_permits: AtomicU32,
}

impl AdaptiveConcurrency {
    pub fn new(max_permits: u32) -> Self {
        let initial = max_permits;
        Self {
            semaphore: Arc::new(Semaphore::new(initial as usize)),
            max_permits,
            current_permits: AtomicU32::new(initial),
        }
    }

    /// Acquire a permit. Blocks until one is available.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed")
    }

    /// Called on successful request — grow permits toward max.
    pub fn on_success(&self) {
        let current = self.current_permits.load(Ordering::SeqCst);
        if current < self.max_permits {
            let new = current + 1;
            if self
                .current_permits
                .compare_exchange(current, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.semaphore.add_permits(1);
                debug!(permits = new, "Adaptive concurrency: grew permits");
            }
        }
    }

    /// Called on rate limit — shrink permits (min 1).
    pub fn on_rate_limit(&self) {
        let current = self.current_permits.load(Ordering::SeqCst);
        if current > 1 {
            let new = (current / 2).max(1);
            let to_remove = current - new;
            if self
                .current_permits
                .compare_exchange(current, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // Acquire and forget permits to reduce the semaphore's capacity
                for _ in 0..to_remove {
                    if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
                        permit.forget();
                    }
                }
                debug!(permits = new, removed = to_remove, "Adaptive concurrency: shrunk permits");
            }
        }
    }

    pub fn current_permits(&self) -> u32 {
        self.current_permits.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_permits() {
        let ac = AdaptiveConcurrency::new(10);
        assert_eq!(ac.current_permits(), 10);
    }

    #[tokio::test]
    async fn test_acquire_and_release() {
        let ac = AdaptiveConcurrency::new(2);
        let p1 = ac.acquire().await;
        let p2 = ac.acquire().await;
        // Both acquired, semaphore should be at 0
        assert_eq!(ac.semaphore.available_permits(), 0);
        drop(p1);
        assert_eq!(ac.semaphore.available_permits(), 1);
        drop(p2);
        assert_eq!(ac.semaphore.available_permits(), 2);
    }

    #[test]
    fn test_on_success_grows() {
        let ac = AdaptiveConcurrency::new(10);
        // Shrink first so we have room to grow
        ac.on_rate_limit(); // 10 -> 5
        let after_shrink = ac.current_permits();
        ac.on_success();
        assert_eq!(ac.current_permits(), after_shrink + 1);
    }

    #[test]
    fn test_on_success_capped_at_max() {
        let ac = AdaptiveConcurrency::new(3);
        ac.on_success();
        ac.on_success();
        assert_eq!(ac.current_permits(), 3); // already at max
    }

    #[test]
    fn test_on_rate_limit_shrinks() {
        let ac = AdaptiveConcurrency::new(10);
        ac.on_rate_limit();
        assert_eq!(ac.current_permits(), 5); // halved
    }

    #[test]
    fn test_on_rate_limit_min_1() {
        let ac = AdaptiveConcurrency::new(1);
        ac.on_rate_limit();
        assert_eq!(ac.current_permits(), 1); // can't go below 1
    }

    #[test]
    fn test_shrink_then_grow_cycle() {
        let ac = AdaptiveConcurrency::new(8);
        ac.on_rate_limit(); // 8 -> 4
        assert_eq!(ac.current_permits(), 4);
        ac.on_rate_limit(); // 4 -> 2
        assert_eq!(ac.current_permits(), 2);
        ac.on_success(); // 2 -> 3
        assert_eq!(ac.current_permits(), 3);
        ac.on_success(); // 3 -> 4
        assert_eq!(ac.current_permits(), 4);
    }

    #[test]
    fn test_multiple_rate_limits_to_minimum() {
        let ac = AdaptiveConcurrency::new(16);
        for _ in 0..10 {
            ac.on_rate_limit();
        }
        assert_eq!(ac.current_permits(), 1);
    }
}
