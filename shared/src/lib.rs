#![cfg_attr(feature = "strict", deny(warnings))]

//! Shared infrastructure for bip300-monitor extractors and tools.

/// Process-wide structured diagnostics.
pub mod logging;

/// JSON rendering of monitor events with byte fields in hexadecimal.
pub mod json;

/// Liveness heartbeat for a service a healthcheck cannot probe over a port.
pub mod liveness;

/// Core NATS connection and publishing infrastructure.
pub mod nats;

/// Stable subjects in the monitor's Core NATS contract.
pub mod nats_subjects;

/// Shared process lifecycle and signal handling.
pub mod process;

/// Commit-then-publish recording of observed events.
pub mod recorder;

/// Protobuf event types shared by extractors and consumers.
pub mod protobuf;

/// The authoritative Postgres record of observed events.
pub mod store;
