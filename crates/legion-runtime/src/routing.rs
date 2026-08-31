//! Deterministic weighted routing between the active and canary artifacts.

use serde::{Deserialize, Serialize};

/// A canary route stored under `/deploy/routes/<function>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionRoute {
    pub name: String,
    pub artifact_cid: String,
    /// Canary traffic share in basis points (`0..=10_000`).
    pub weight: u16,
    pub updated_at: i64,
}

impl FunctionRoute {
    pub fn validate(&self) -> bool {
        self.weight <= 10_000 && !self.artifact_cid.is_empty()
    }

    /// Select the canary deterministically for a call ID.
    pub fn selects_canary(&self, call_id: &str) -> bool {
        if self.weight == 0 {
            return false;
        }
        if self.weight >= 10_000 {
            return true;
        }
        let digest = blake3::hash(call_id.as_bytes());
        let bucket = u16::from_be_bytes([digest.as_bytes()[0], digest.as_bytes()[1]]) as u32;
        bucket * 10_000 / 65_536 < u32::from(self.weight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(weight: u16) -> FunctionRoute {
        FunctionRoute {
            name: "hello".into(),
            artifact_cid: "cid".into(),
            weight,
            updated_at: 0,
        }
    }

    #[test]
    fn boundary_weights_are_exact() {
        assert!(!route(0).selects_canary("call"));
        assert!(route(10_000).selects_canary("call"));
        assert!(!route(10_001).validate());
    }

    #[test]
    fn selection_is_stable_and_approximately_weighted() {
        let route = route(2_500);
        let selected = (0..10_000)
            .filter(|index| route.selects_canary(&format!("call-{index}")))
            .count();
        assert!((2_350..=2_650).contains(&selected), "selected {selected}");
        assert_eq!(
            route.selects_canary("stable-call"),
            route.selects_canary("stable-call"),
        );
    }
}
