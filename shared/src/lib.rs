#![cfg_attr(feature = "strict", deny(warnings))]

//! Shared infrastructure for bip300-monitor extractors and tools.

/// Core NATS connection and publishing infrastructure.
pub mod nats;

/// Stable subjects in the monitor's Core NATS contract.
pub mod nats_subjects;

/// Protobuf event types shared by extractors and consumers.
pub mod protobuf;
