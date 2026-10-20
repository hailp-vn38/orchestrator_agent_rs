//! Dữ liệu đầu vào để xuất bản artifact.

use std::fmt;
use std::pin::Pin;

use tokio::io::AsyncRead;

use crate::types::ArtifactRole;

pub(super) enum InputBody {
    Bytes(Vec<u8>),
    Reader(Pin<Box<dyn AsyncRead + Send>>),
}

/// Dữ liệu đầu vào để xuất bản một artifact bất biến qua [`crate::ToolArtifacts::put`].
/// Hỗ trợ truyền từ bộ nhớ hoặc stream qua [`AsyncRead`] mà không ép giữ toàn bộ file trong RAM.
pub struct ArtifactInput {
    pub name: String,
    pub mime_type: String,
    pub role: ArtifactRole,
    pub(super) body: InputBody,
}

impl fmt::Debug for ArtifactInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArtifactInput")
            .field("name", &self.name)
            .field("mime_type", &self.mime_type)
            .field("role", &self.role)
            .finish()
    }
}

impl ArtifactInput {
    pub fn from_bytes(
        name: impl Into<String>,
        mime_type: impl Into<String>,
        role: ArtifactRole,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            name: name.into(),
            mime_type: mime_type.into(),
            role,
            body: InputBody::Bytes(bytes.into()),
        }
    }

    pub fn primary_bytes(
        name: impl Into<String>,
        mime_type: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self::from_bytes(name, mime_type, ArtifactRole::Primary, bytes)
    }

    pub fn intermediate_bytes(
        name: impl Into<String>,
        mime_type: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self::from_bytes(name, mime_type, ArtifactRole::Intermediate, bytes)
    }

    pub fn from_reader<R>(
        name: impl Into<String>,
        mime_type: impl Into<String>,
        role: ArtifactRole,
        reader: R,
    ) -> Self
    where
        R: AsyncRead + Send + 'static,
    {
        Self {
            name: name.into(),
            mime_type: mime_type.into(),
            role,
            body: InputBody::Reader(Box::pin(reader)),
        }
    }
}
