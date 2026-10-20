//! G0 spike (GAP-04): minimal, throwaway verification code. Not the M0 library shell.
//!
//! Covers: typed config + routing fingerprint, gateway normalization over `llm` 1.3.8
//! stream chunks, "full-response Finished" collector, owner lock (fs4), deployment
//! profile validation, SQLite migration/contention probe.

pub mod gateway;
pub mod profile;
pub mod store;
