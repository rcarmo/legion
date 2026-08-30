use std::{
    collections::HashMap,
    sync::{Mutex, atomic::{AtomicU64, Ordering}},
    time::{Duration, Instant},
};

use uuid::Uuid;

use crate::config::SessionRateLimit;

pub struct SessionRateLimiter {
    config: SessionRateLimit,
    windows: Mutex<HashMap<Uuid, Window>>,
    rejections: AtomicU64,
}

struct Window {
    started: Instant,
    count: u32,
}

impl SessionRateLimiter {
    pub fn new(config: SessionRateLimit) -> Self {
        Self {
            config,
            windows: Mutex::new(HashMap::new()),
            rejections: AtomicU64::new(0),
        }
    }

    pub fn check(&self, run_id: Uuid) -> Result<(), u64> {
        if self.config.max_requests_per_window == 0 || self.config.window_ms == 0 {
            return Ok(());
        }
        let now = Instant::now();
        let duration = Duration::from_millis(self.config.window_ms);
        let mut windows = self.windows.lock().expect("session rate lock poisoned");
        windows.retain(|_, window| now.duration_since(window.started) < duration);
        let window = windows.entry(run_id).or_insert(Window { started: now, count: 0 });
        let elapsed = now.duration_since(window.started);
        if elapsed >= duration {
            window.started = now;
            window.count = 0;
        }
        if window.count >= self.config.max_requests_per_window {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return Err(duration.saturating_sub(now.duration_since(window.started))
                .as_millis().max(1) as u64);
        }
        window.count += 1;
        Ok(())
    }

    pub fn rejections(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_have_independent_windows() {
        let limiter = SessionRateLimiter::new(SessionRateLimit {
            max_requests_per_window: 1,
            window_ms: 60_000,
        });
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        assert!(limiter.check(first).is_ok());
        assert!(limiter.check(first).is_err());
        assert!(limiter.check(second).is_ok());
    }

    #[test]
    fn zero_disables_session_rate_limit() {
        let limiter = SessionRateLimiter::new(SessionRateLimit {
            max_requests_per_window: 0,
            window_ms: 60_000,
        });
        let run_id = Uuid::new_v4();
        for _ in 0..100 {
            assert!(limiter.check(run_id).is_ok());
        }
    }
}
