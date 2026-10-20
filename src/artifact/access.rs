//! Đọc artifact: kiểm tra scope (host/tool/session family) và xác minh toàn vẹn.

use sha2::{Digest, Sha256};
use tokio::fs::File;

use super::errors::*;
use super::reader::{ArtifactDownload, ArtifactReader};
use super::store::ArtifactStore;
use super::validate::is_safe_storage_key;
use super::STREAM_CHUNK_BYTES;
use crate::error::{ErrorCode, OrchestratorError};
use crate::store::StoredArtifact;
use crate::tool::{ToolError, ToolScope};
use crate::types::{ArtifactRef, CallerContext};

impl ArtifactStore {
    /// Mở artifact cho host (`Orchestrator::open_artifact`): kiểm tra owner/workspace, kiểm tra
    /// `download_max_bytes` và kiểm tra file vật lý tồn tại + đúng kích thước.
    pub(crate) async fn open_for_host(
        &self,
        caller: &CallerContext,
        artifact_id: &str,
    ) -> Result<ArtifactDownload, OrchestratorError> {
        let stored = self
            .load_stored(artifact_id)
            .await?
            .ok_or_else(|| OrchestratorError::new(ErrorCode::NotFound, "artifact not found"))?;
        if stored.owner_id != caller.owner_id || stored.workspace_id != caller.workspace_id {
            return Err(OrchestratorError::new(
                ErrorCode::PermissionDenied,
                "the artifact is outside the caller's scope",
            ));
        }
        if stored.artifact.size_bytes > self.limits.download_max_bytes {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "artifact exceeds download_max_bytes",
            ));
        }
        let reader = self
            .open_verified_reader(
                stored.artifact.clone(),
                &stored.storage_key,
                self.limits.download_max_bytes,
            )
            .await?;
        Ok(ArtifactDownload {
            artifact: stored.artifact,
            reader,
        })
    }

    /// Mở artifact cho `HostTool` (`ToolContext::artifacts().open`):
    /// Enforce:
    /// 1) Cùng `owner_id` và `workspace_id`.
    /// 2) Session tạo artifact thuộc cùng session family (`scope.root_session_id`).
    ///    Cùng workspace nhưng khác session family bị từ chối!
    /// 3) Explicit invocation input / checkpoint ancestry:
    ///    `artifact_id` phải do chính invocation hiện tại tạo ra, hoặc được truyền tường minh
    ///    trong `arguments_json` của invocation hiện tại, hoặc là `checkpoint_artifact_id`
    ///    của invocation hiện tại (hoặc được tham chiếu bên trong manifest của `checkpoint_artifact_id`).
    pub(crate) async fn open_for_tool(
        &self,
        scope: &ToolScope,
        artifact_id: &str,
    ) -> Result<ArtifactReader, ToolError> {
        let stored = self
            .load_stored(artifact_id)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not read artifact metadata".into(),
            })?
            .ok_or_else(|| ToolError {
                code: CODE_ARTIFACT_NOT_FOUND.into(),
                message: "artifact not found".into(),
            })?;

        self.authorize_tool_access(scope, &stored).await?;

        self.open_verified_reader(stored.artifact, &stored.storage_key, self.limits.max_bytes)
            .await
            .map_err(|e| match e.code() {
                ErrorCode::InvalidInput => ToolError {
                    code: CODE_ARTIFACT_TOO_LARGE.into(),
                    message: e.message().to_owned(),
                },
                _ => ToolError {
                    code: CODE_ARTIFACT_INTEGRITY.into(),
                    message: e.message().to_owned(),
                },
            })
    }

    /// Xác minh một artifact tồn tại, thuộc đúng scope của invocation và có bytes nguyên vẹn
    /// trên đĩa trước khi cho phép CAS vào `tool_invocations.checkpoint_artifact_id`.
    pub(crate) async fn verify_for_checkpoint_commit(
        &self,
        scope: &ToolScope,
        manifest_id: &str,
    ) -> Result<(), ToolError> {
        let mut reader = self.open_for_tool(scope, manifest_id).await?;
        while reader
            .read_chunk(STREAM_CHUNK_BYTES)
            .await
            .map_err(|e| ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            })?
            .is_some()
        {}
        Ok(())
    }

    /// Xác minh hoàn chỉnh một artifact trong cùng session family (dùng cho `checkpoint().current()`
    /// và `resolve_interrupted_invocation`).
    pub(crate) async fn verify_in_family(
        &self,
        owner_id: &str,
        workspace_id: &str,
        root_session_id: &str,
        artifact_id: &str,
    ) -> Result<ArtifactRef, ToolError> {
        let stored = self
            .load_stored(artifact_id)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not read artifact metadata".into(),
            })?
            .ok_or_else(|| ToolError {
                code: CODE_ARTIFACT_NOT_FOUND.into(),
                message: "artifact not found".into(),
            })?;
        if stored.owner_id != owner_id
            || stored.workspace_id != workspace_id
            || stored.root_session_id != root_session_id
        {
            return Err(ToolError {
                code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                message: "artifact is outside the session family scope".into(),
            });
        }
        let mut reader = self
            .open_verified_reader(
                stored.artifact.clone(),
                &stored.storage_key,
                self.limits.download_max_bytes,
            )
            .await
            .map_err(|e| ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            })?;
        while reader
            .read_chunk(STREAM_CHUNK_BYTES)
            .await
            .map_err(|e| ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            })?
            .is_some()
        {}
        Ok(stored.artifact)
    }

    pub(super) async fn authorize_tool_access(
        &self,
        scope: &ToolScope,
        stored: &StoredArtifact,
    ) -> Result<(), ToolError> {
        let deny = |msg: &str| ToolError {
            code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
            message: msg.to_owned(),
        };
        if stored.owner_id != scope.owner_id || stored.workspace_id != scope.workspace_id {
            return Err(deny("artifact belongs to a different owner or workspace"));
        }
        // Cùng workspace không tự cấp quyền đọc: phải thuộc cùng session family.
        if stored.root_session_id != scope.root_session_id {
            return Err(deny("artifact belongs to a different session family"));
        }
        // Chính invocation hiện tại tạo ra artifact này.
        if stored.producer_invocation_id.as_deref() == Some(scope.invocation_id.as_str()) {
            return Ok(());
        }

        let (args_json, checkpoint_id) = self
            .db
            .invocation_input_scope(&scope.invocation_id, &scope.session_id, &scope.run_id)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not verify invocation input scope".into(),
            })?
            .ok_or_else(|| deny("invocation is not active in this session"))?;

        let target_id = stored.artifact.id.as_str();
        if json_mentions_id(&args_json, target_id) {
            return Ok(());
        }
        if checkpoint_id.as_deref() == Some(target_id) {
            return Ok(());
        }
        // Nếu invocation đã có committed checkpoint manifest, cho phép đọc các artifact IDs
        // được ghi bên trong manifest đó (ví dụ các completed item refs khi resume).
        if let Some(cp_id) = checkpoint_id {
            if let Ok(Some(cp_stored)) = self.load_stored(&cp_id).await {
                if cp_stored.owner_id == scope.owner_id
                    && cp_stored.workspace_id == scope.workspace_id
                    && cp_stored.root_session_id == scope.root_session_id
                {
                    if let Ok(reader) = self
                        .open_verified_reader(
                            cp_stored.artifact,
                            &cp_stored.storage_key,
                            self.limits.max_bytes,
                        )
                        .await
                    {
                        if let Ok(text) = reader.read_to_string().await {
                            if json_mentions_id(&text, target_id) {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        Err(deny(
            "artifact is not in the invocation's explicit input or checkpoint scope",
        ))
    }

    pub(super) async fn open_verified_reader(
        &self,
        artifact: ArtifactRef,
        storage_key: &str,
        max_bytes: u64,
    ) -> Result<ArtifactReader, OrchestratorError> {
        if !is_safe_storage_key(storage_key) {
            return Err(integrity_orchestrator_error("invalid artifact storage key"));
        }
        if artifact.size_bytes > max_bytes {
            return Err(OrchestratorError::new(
                ErrorCode::InvalidInput,
                "artifact exceeds the configured read byte limit",
            ));
        }
        let path = self.committed_dir.join(storage_key);
        let file = File::open(&path)
            .await
            .map_err(|_| integrity_orchestrator_error("committed artifact file is missing"))?;
        let meta = file
            .metadata()
            .await
            .map_err(|_| integrity_orchestrator_error("could not stat committed artifact file"))?;
        if !meta.is_file() || meta.len() != artifact.size_bytes {
            return Err(integrity_orchestrator_error(
                "committed artifact file size does not match metadata",
            ));
        }
        Ok(ArtifactReader {
            artifact,
            file,
            max_bytes,
            read_so_far: 0,
            hasher: Sha256::new(),
            verified: false,
        })
    }

    pub(super) async fn load_stored(
        &self,
        artifact_id: &str,
    ) -> Result<Option<StoredArtifact>, OrchestratorError> {
        self.db.artifact_load(artifact_id).await
    }

    /// Đọc một đoạn văn bản UTF-8 có giới hạn cho built-in tool `artifact_read` của model
    /// trong phạm vi cùng `(owner_id, workspace_id, root_session_id)` (docs/17 §17.3, §17.5).
    /// Luôn xác minh toàn vẹn SHA-256 của file committed trước khi trích xuất đoạn con.
    pub(crate) async fn read_model_excerpt(
        &self,
        scope: &ToolScope,
        artifact_id: &str,
        offset_bytes: u64,
        max_bytes: usize,
    ) -> Result<(ArtifactRef, String, bool), ToolError> {
        let stored = self
            .load_stored(artifact_id)
            .await
            .map_err(|_| ToolError {
                code: CODE_ARTIFACT_UNAVAILABLE.into(),
                message: "could not read artifact metadata".into(),
            })?
            .ok_or_else(|| ToolError {
                code: CODE_ARTIFACT_NOT_FOUND.into(),
                message: "artifact not found".into(),
            })?;
        if stored.owner_id != scope.owner_id
            || stored.workspace_id != scope.workspace_id
            || stored.root_session_id != scope.root_session_id
        {
            return Err(ToolError {
                code: CODE_ARTIFACT_PERMISSION_DENIED.into(),
                message: "artifact is outside the session family scope".into(),
            });
        }
        let reader = self
            .open_verified_reader(
                stored.artifact.clone(),
                &stored.storage_key,
                self.limits.max_bytes,
            )
            .await
            .map_err(|e| ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            })?;
        let text = reader.read_to_string().await.map_err(|e| match e.code() {
            ErrorCode::InvalidInput => ToolError {
                code: CODE_ARTIFACT_INVALID_INPUT.into(),
                message: e.message().to_owned(),
            },
            _ => ToolError {
                code: CODE_ARTIFACT_INTEGRITY.into(),
                message: e.message().to_owned(),
            },
        })?;
        let total = text.len();
        let mut start = usize::try_from(offset_bytes).unwrap_or(total).min(total);
        while start > 0 && !text.is_char_boundary(start) {
            start -= 1;
        }
        let mut end = start.saturating_add(max_bytes).min(total);
        while end > start && !text.is_char_boundary(end) {
            end -= 1;
        }
        let has_more = end < total;
        Ok((stored.artifact, text[start..end].to_owned(), has_more))
    }
}

/// Kiểm tra một chuỗi JSON có chứa đúng string literal `target_id` ở bất kỳ trường nào hay không.
pub(super) fn json_mentions_id(raw_json: &str, target_id: &str) -> bool {
    if target_id.is_empty() {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw_json) else {
        return false;
    };
    value_mentions_id(&value, target_id)
}

fn value_mentions_id(v: &serde_json::Value, target_id: &str) -> bool {
    match v {
        serde_json::Value::String(s) => s == target_id,
        serde_json::Value::Array(items) => items.iter().any(|x| value_mentions_id(x, target_id)),
        serde_json::Value::Object(map) => map.values().any(|x| value_mentions_id(x, target_id)),
        _ => false,
    }
}
