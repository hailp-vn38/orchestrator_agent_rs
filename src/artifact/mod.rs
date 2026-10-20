//! `ArtifactStore` (docs/17, docs/18 §18.7): module sâu nội bộ quản lý toàn bộ vòng đời
//! artifact trên local filesystem + bảng metadata `artifacts` trong SQLite.
//!
//! Bất biến cốt lõi:
//! - Không bao giờ ghép tên file từ user/tool/model vào đường dẫn vật lý: cả file staging lẫn
//!   file committed đều dùng khóa opaque do server sinh.
//! - Luồng xuất bản bất biến: ghi vào `staging/<tmp>.part` kèm kiểm tra kích thước + băm
//!   SHA-256 tăng dần → `sync_all()` → `rename` nguyên tử sang `committed/<storage_key>` trên
//!   cùng filesystem → `sync_all()` thư mục committed → ghi row `artifacts` và tăng
//!   `sessions.state_revision` trong một write transaction SQLite.
//! - Dữ liệu đang staging hoặc đã rename nhưng chưa commit metadata vào SQLite không bao giờ
//!   được đọc qua bất kỳ API nào; `sweep_orphans` dọn các file mồ côi đã quá `staging_grace`
//!   mà không bao giờ xóa các artifact đã có metadata trong SQLite (MVP không có TTL/GC).
//! - Khi đọc (`open_for_host` / `open_for_tool`), kiểm tra quyền sở hữu/ancestry/explicit input
//!   scope, kiểm tra kích thước file khi mở và xác minh SHA-256 trên luồng byte đọc ra; thiếu
//!   hoặc hỏng byte luôn trả lỗi toàn vẹn, không bao giờ báo thành công giả.

mod access;
mod backup;
mod cursor;
mod errors;
mod fault;
mod fsutil;
mod input;
mod limits;
mod listing;
mod publish;
mod reader;
mod row;
mod scope;
mod store;
mod validate;

#[cfg(test)]
mod tests;

#[cfg(feature = "test-support")]
pub use fault::ArtifactFault;
pub use input::ArtifactInput;
pub use reader::{ArtifactDownload, ArtifactReader};

#[allow(unused_imports)]
pub(crate) use errors::*;
pub(crate) use limits::ArtifactLimits;
pub(crate) use scope::ArtifactWriteScope;
pub(crate) use store::ArtifactStore;

pub(crate) const ARTIFACTS_DIR: &str = "artifacts";
pub(crate) const STAGING_DIR: &str = "staging";
pub(crate) const COMMITTED_DIR: &str = "committed";
const STREAM_CHUNK_BYTES: usize = 64 * 1024;
const MAX_NAME_CHARS: usize = 255;
const MAX_MIME_CHARS: usize = 128;
const CURSOR_VERSION: &str = "a1";
