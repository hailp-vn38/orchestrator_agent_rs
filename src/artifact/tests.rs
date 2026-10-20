//! Tests tích hợp cho `ArtifactStore`.

use super::errors::{CODE_ARTIFACT_INTEGRITY, CODE_ARTIFACT_UNAVAILABLE};
use super::input::ArtifactInput;
use super::limits::ArtifactLimits;
use super::scope::ArtifactWriteScope;
use super::store::ArtifactStore;
use crate::config::RuntimeConfig;
use crate::error::ErrorCode;
use crate::store::{RunRoute, SessionStore, StoredToolCall, DB_FILE};
use crate::types::{ArtifactRole, CallerContext, PageRequest};
use std::time::Duration;

fn caller() -> CallerContext {
    CallerContext::new("owner-1", "ws-1")
}

fn route(_: &str) -> Option<RunRoute> {
    Some(RunRoute {
        provider_id: "p".into(),
        model_id: "m".into(),
        fingerprint: "v1:fp".into(),
    })
}

async fn setup() -> (
    tempfile::TempDir,
    SessionStore,
    ArtifactStore,
    ArtifactWriteScope,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::open(&dir.path().join(DB_FILE)).await.unwrap();
    let limits = ArtifactLimits::from_runtime(&RuntimeConfig::default());
    let artifacts = ArtifactStore::open(store.clone(), dir.path(), limits, None)
        .await
        .unwrap();
    let sid = store.create_root_session(&caller(), "root").await.unwrap();
    let turn = store
        .admit_turn(&caller(), &sid, "hello", route)
        .await
        .unwrap();
    let run_id = store.running_run_ids().await.unwrap().remove(0);
    let calls = vec![StoredToolCall {
        id: "call-1".into(),
        name: "writer".into(),
        arguments: serde_json::json!({}),
    }];
    let inv = store
        .commit_tool_calls(&run_id, "", &calls)
        .await
        .unwrap()
        .unwrap()
        .remove(0);
    assert!(store.begin_invocation(&run_id, &inv.id).await.unwrap());
    let scope = ArtifactWriteScope {
        owner_id: "owner-1".into(),
        workspace_id: "ws-1".into(),
        session_id: sid,
        turn_id: turn.turn_id,
        run_id,
        producer_invocation_id: Some(inv.id),
    };
    (dir, store, artifacts, scope)
}

#[tokio::test]
async fn put_open_and_list_roundtrip_with_integrity_verification() {
    let (_dir, _store, artifacts, scope) = setup().await;
    let payload = b"# Heading\n\nArtifact body content.\n";
    let r = artifacts
        .put(
            &scope,
            ArtifactInput::primary_bytes("report.md", "text/markdown", payload.to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(r.name, "report.md");
    assert_eq!(r.mime_type, "text/markdown");
    assert_eq!(r.size_bytes, payload.len() as u64);
    assert_eq!(r.role, ArtifactRole::Primary);

    let page = artifacts
        .list_for_turn(
            &caller(),
            &scope.session_id,
            &scope.turn_id,
            PageRequest::first(10),
        )
        .await
        .unwrap();
    assert_eq!(page.items, vec![r.clone()]);
    assert_eq!(page.next_cursor, None);

    let dl = artifacts.open_for_host(&caller(), &r.id).await.unwrap();
    assert_eq!(dl.metadata(), &r);
    let read_back = dl.read_all().await.unwrap();
    assert_eq!(read_back, payload);

    // Sửa 1 byte trên đĩa -> phát hiện lỗi SHA-256 ngay khi đọc, không trả về thành công giả.
    let mut entries = std::fs::read_dir(artifacts.committed_dir()).unwrap();
    let file_path = entries.next().unwrap().unwrap().path();
    std::fs::write(&file_path, b"# Heading\n\nArtifact body CORRUPT.\n").unwrap();
    let dl_corrupt = artifacts.open_for_host(&caller(), &r.id).await.unwrap();
    let err = dl_corrupt.read_all().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert!(err.message().contains(CODE_ARTIFACT_INTEGRITY));
}

#[tokio::test]
async fn fault_injection_and_orphan_sweep_preserve_committed_artifacts() {
    let (_dir, _store, artifacts, scope) = setup().await;
    let committed_ref = artifacts
        .put(
            &scope,
            ArtifactInput::primary_bytes("keep.txt", "text/plain", b"committed".to_vec()),
        )
        .await
        .unwrap();

    // 1) Lỗi giữa chừng khi staging -> để lại file `.part` trong `staging/`.
    artifacts.fault().set_fail_during_staging(true);
    let err = artifacts
        .put(
            &scope,
            ArtifactInput::primary_bytes("stage-fail.txt", "text/plain", b"partial".to_vec()),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, "STAGING_FAULT");
    artifacts.fault().set_fail_during_staging(false);
    assert_eq!(
        std::fs::read_dir(artifacts.staging_dir()).unwrap().count(),
        1
    );

    // 2) Lỗi sau khi rename sang `committed/` nhưng trước khi ghi SQLite metadata.
    artifacts
        .fault()
        .set_fail_after_rename_before_metadata(true);
    let err2 = artifacts
        .put(
            &scope,
            ArtifactInput::primary_bytes("orphan.txt", "text/plain", b"orphan".to_vec()),
        )
        .await
        .unwrap_err();
    assert_eq!(err2.code, CODE_ARTIFACT_UNAVAILABLE);
    artifacts
        .fault()
        .set_fail_after_rename_before_metadata(false);
    // Có 2 file trong `committed/` nhưng chỉ có 1 row trong SQLite.
    assert_eq!(
        std::fs::read_dir(artifacts.committed_dir())
            .unwrap()
            .count(),
        2
    );
    let page = artifacts
        .list_for_turn(
            &caller(),
            &scope.session_id,
            &scope.turn_id,
            PageRequest::first(10),
        )
        .await
        .unwrap();
    assert_eq!(page.items, vec![committed_ref.clone()]);

    // 3) Sweep với grace lớn -> chưa xóa file mới tạo.
    assert_eq!(
        artifacts
            .sweep_orphans(Duration::from_secs(3600))
            .await
            .unwrap(),
        0
    );

    // 4) Sweep với grace = 0 -> xóa đúng 1 file staging + 1 file committed mồ côi;
    //    artifact đã commit trong SQLite được giữ nguyên vẹn!
    assert_eq!(artifacts.sweep_orphans(Duration::ZERO).await.unwrap(), 2);
    assert_eq!(
        std::fs::read_dir(artifacts.staging_dir()).unwrap().count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(artifacts.committed_dir())
            .unwrap()
            .count(),
        1
    );
    let bytes = artifacts
        .open_for_host(&caller(), &committed_ref.id)
        .await
        .unwrap()
        .read_all()
        .await
        .unwrap();
    assert_eq!(bytes, b"committed");
}
