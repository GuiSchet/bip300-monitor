#![cfg_attr(feature = "strict", deny(warnings))]

//! Live event extractor for the BIP300/301 enforcer.

pub mod client;
pub mod convert;
pub mod proto;

pub use client::EnforcerClient;
