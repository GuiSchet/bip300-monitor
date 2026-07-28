#![cfg_attr(feature = "strict", deny(warnings))]

//! Live event extractor for the BIP300/301 enforcer.

pub mod client;
pub mod config;
pub mod convert;
pub mod proto;
pub mod snapshot;

pub use client::EnforcerClient;
pub use config::Args;
pub use snapshot::publish_initial_snapshot;
