//! `LlmGateway`: internal seam giữa runtime và provider protocol (docs/08, docs/18 §18.5).
//!
//! Production dùng [`ClientGateway`] trên crate `llm` (pin `=1.3.8`, evidence G0). Type của
//! `llm` không bao giờ ra public API. M2: tool definitions/calls/results. M4: stream được lắp
//! ráp thành một response duy nhất; text delta và `ToolCallReady` chỉ là preview qua
//! [`StreamSink`] (không persist, không kích hoạt execution).
//!
//! Item ở đây là `pub` trong module private; chỉ re-export qua `test_support` khi bật
//! feature `test-support`, để production host không thể inject/lắp ráp gateway.

mod client;
mod collect;
mod convert;
mod error;
mod runtime;
#[cfg(test)]
mod tests;
mod types;

pub use client::ClientGateway;
pub use error::GatewayError;
pub(crate) use runtime::RuntimeGateway;
pub use runtime::{ProviderRuntime, RuntimeProvider};
pub use types::*;
