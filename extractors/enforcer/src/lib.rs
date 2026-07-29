#![cfg_attr(feature = "strict", deny(warnings))]

//! Live event extractor for the BIP300/301 enforcer.

pub mod client;
pub mod config;
pub mod convert;
mod event;
pub mod proto;
pub mod runtime;
mod snapshot;

pub use client::EnforcerClient;
pub use config::Args;
pub use runtime::run;
pub use shared::logging;
pub use shared::logging::LogLevel;
