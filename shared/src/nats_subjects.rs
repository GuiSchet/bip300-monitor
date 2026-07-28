//! Stable Core NATS subjects used by monitor publishers and consumers.

use std::fmt;

/// All normalized enforcer events.
pub const ENFORCER_EVENTS: &str = "bip300.enforcer";

/// Known subjects in the monitor's Core NATS contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Subject {
    /// Chain state, sidechain snapshots, and live block events.
    Enforcer,
}

impl Subject {
    /// Return the stable subject string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enforcer => ENFORCER_EVENTS,
        }
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::{ENFORCER_EVENTS, Subject};

    #[test]
    fn enforcer_subject_is_stable() {
        assert_eq!(Subject::Enforcer.as_str(), ENFORCER_EVENTS);
        assert_eq!(Subject::Enforcer.to_string(), "bip300.enforcer");
    }
}
