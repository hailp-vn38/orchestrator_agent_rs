//! Read API (fake LLM + SQLite thật): `list_sessions`, `search_sessions`, `get_session`,
//! `list_runs`, `get_run`, `list_turns`, `search_history` qua public `Orchestrator`.
//!
//! Mode: deterministic fake gateway (`test-support`) — KHÔNG phải evidence provider live.

use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_rs::test_support::ScriptedGateway;
use orchestrator_agent_rs::{
    AgentDefinition, AgentMode, CallerContext, ErrorCode, HistorySearch, LlmBackend, LlmConfig,
    LlmProviderConfig, MessageRole, Orchestrator, OrchestratorConfig, PageRequest, ProviderAuth,
    RunStatus, SecretString, SessionFilter, SessionSearch,
};

struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("root.md"), "You are the root test agent.").unwrap();
        std::fs::write(dir.path().join("other.md"), "You are the other agent.").unwrap();
        Self { dir }
    }

    async fn build(&self, gw: &Arc<ScriptedGateway>) -> Orchestrator {
        let mut cfg = OrchestratorConfig::new(
            LlmConfig {
                providers: vec![LlmProviderConfig {
                    id: "fake".into(),
                    backend: LlmBackend::OpenAi,
                    endpoint: None,
                    auth: ProviderAuth::ApiKey(SecretString::new("sk-test-not-real")),
                    request_timeout: Duration::from_secs(30),
                }],
            },
            vec![
                AgentDefinition::new("root", "fake", "model-x")
                    .with_mode(AgentMode::Primary)
                    .with_prompt_path(self.dir.path().join("root.md")),
                AgentDefinition::new("other", "fake", "model-x")
                    .with_mode(AgentMode::Primary)
                    .with_prompt_path(self.dir.path().join("other.md")),
            ],
            self.dir.path().join("store"),
        );
        cfg.runtime.page_size = 2;
        Orchestrator::builder(cfg)
            .with_gateway(gw.clone())
            .build()
            .await
            .unwrap()
    }
}

fn alice() -> CallerContext {
    CallerContext::new("alice", "ws1")
}

fn bob() -> CallerContext {
    CallerContext::new("bob", "ws1")
}

async fn run_turn(o: &Orchestrator, c: &CallerContext, session: &str, text: &str) -> String {
    let turn = o.submit(c, session, text.into()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let r = o.result(c, session, &turn.turn_id).await.unwrap();
        if r.status != RunStatus::Running {
            return turn.turn_id;
        }
        assert!(tokio::time::Instant::now() < deadline, "run stuck");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn sessions_list_filter_search_and_get() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = env.build(&gw).await;

    let s1 = o.create_session(&alice(), "root").await.unwrap();
    let s2 = o.create_session(&alice(), "other").await.unwrap();
    let s3 = o.create_session(&alice(), "root").await.unwrap();
    let foreign = o.create_session(&bob(), "root").await.unwrap();
    gw.push_text("banana answer");
    run_turn(&o, &alice(), &s1, "tell me about 100%_fruit").await;
    o.archive_session(&alice(), &s3).await.unwrap();

    // Newest first, scoped to caller, paged by page_size = 2.
    let p1 = o
        .list_sessions(&alice(), &SessionFilter::default(), PageRequest::first(10))
        .await
        .unwrap();
    let ids: Vec<_> = p1.items.iter().map(|s| s.session_id.as_str()).collect();
    assert_eq!(ids, [s3.as_str(), s2.as_str()]);
    let p2 = o
        .list_sessions(
            &alice(),
            &SessionFilter::default(),
            PageRequest::after(p1.next_cursor.unwrap(), 10),
        )
        .await
        .unwrap();
    assert_eq!(p2.items.len(), 1);
    assert_eq!(p2.items[0].session_id, s1);
    assert!(p2.next_cursor.is_none());
    assert!(!ids.contains(&foreign.as_str()));

    let archived = o
        .list_sessions(
            &alice(),
            &SessionFilter {
                archived: Some(true),
                ..Default::default()
            },
            PageRequest::first(10),
        )
        .await
        .unwrap();
    assert_eq!(archived.items.len(), 1);
    assert!(archived.items[0].archived);

    let by_agent = o
        .list_sessions(
            &alice(),
            &SessionFilter {
                agent_id: Some("other".into()),
                ..Default::default()
            },
            PageRequest::first(10),
        )
        .await
        .unwrap();
    assert_eq!(by_agent.items.len(), 1);
    assert_eq!(by_agent.items[0].session_id, s2);

    // Search content; LIKE wildcards in the keyword are literal.
    let hit = o
        .search_sessions(
            &alice(),
            &SessionSearch::new("BANANA"),
            PageRequest::first(10),
        )
        .await
        .unwrap();
    assert_eq!(hit.items.len(), 1);
    assert_eq!(hit.items[0].session_id, s1);
    let literal = o
        .search_sessions(
            &alice(),
            &SessionSearch::new("100%_f"),
            PageRequest::first(10),
        )
        .await
        .unwrap();
    assert_eq!(literal.items.len(), 1);
    let wildcard_only = o
        .search_sessions(&alice(), &SessionSearch::new("%"), PageRequest::first(10))
        .await
        .unwrap();
    assert_eq!(
        wildcard_only.items.len(),
        1,
        "only the message with a literal %"
    );
    assert!(o
        .search_sessions(
            &bob(),
            &SessionSearch::new("banana"),
            PageRequest::first(10)
        )
        .await
        .unwrap()
        .items
        .is_empty());
    let empty = o
        .search_sessions(&alice(), &SessionSearch::new("  "), PageRequest::first(10))
        .await
        .unwrap_err();
    assert_eq!(empty.code(), ErrorCode::InvalidInput);

    let got = o.get_session(&alice(), &s1).await.unwrap();
    assert_eq!(got.agent_id, "root");
    assert!(got.parent_session_id.is_none() && !got.archived);
    assert_eq!(
        o.get_session(&alice(), &foreign).await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
    assert_eq!(
        o.get_session(&alice(), "nope").await.unwrap_err().code(),
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn runs_turns_and_history_search() {
    let env = Env::new();
    let gw = ScriptedGateway::new();
    let o = env.build(&gw).await;

    let s1 = o.create_session(&alice(), "root").await.unwrap();
    let s2 = o.create_session(&alice(), "root").await.unwrap();
    gw.push_text("first reply about apples");
    let t1 = run_turn(&o, &alice(), &s1, "question one").await;
    gw.push_text("second reply");
    let t2 = run_turn(&o, &alice(), &s1, "question two about Apples").await;
    gw.push_text("other session apples");
    run_turn(&o, &alice(), &s2, "hello").await;

    // Turns: creation order, input + output, paged (page_size = 2).
    let turns = o
        .list_turns(&alice(), &s1, PageRequest::first(10))
        .await
        .unwrap();
    assert_eq!(turns.items.len(), 2);
    assert_eq!(turns.items[0].turn_id, t1);
    assert_eq!(turns.items[0].input.as_deref(), Some("question one"));
    assert_eq!(
        turns.items[0].output.as_deref(),
        Some("first reply about apples")
    );
    assert_eq!(turns.items[0].attempts, 1);
    assert_eq!(turns.items[1].turn_id, t2);
    assert!(turns.next_cursor.is_none());
    let first = o
        .list_turns(&alice(), &s1, PageRequest::first(1))
        .await
        .unwrap();
    assert_eq!(first.items.len(), 1);
    let second = o
        .list_turns(
            &alice(),
            &s1,
            PageRequest::after(first.next_cursor.unwrap(), 1),
        )
        .await
        .unwrap();
    assert_eq!(second.items[0].turn_id, t2);
    assert!(o
        .list_turns(&bob(), &s1, PageRequest::first(1))
        .await
        .is_err());

    // Cursor of one query is rejected by another.
    let runs_cursor = o
        .list_runs(&alice(), &s1, None, PageRequest::first(1))
        .await
        .unwrap()
        .next_cursor
        .unwrap();
    assert_eq!(
        o.list_turns(&alice(), &s1, PageRequest::after(runs_cursor, 1))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::InvalidInput
    );

    // Runs + get_run.
    let runs = o
        .list_runs(&alice(), &s1, None, PageRequest::first(10))
        .await
        .unwrap();
    assert_eq!(runs.items.len(), 2);
    assert!(runs
        .items
        .iter()
        .all(|r| r.status == RunStatus::Completed && r.attempt == 0));
    let only_t2 = o
        .list_runs(&alice(), &s1, Some(&t2), PageRequest::first(10))
        .await
        .unwrap();
    assert_eq!(only_t2.items.len(), 1);
    assert_eq!(only_t2.items[0].turn_id, t2);
    let run = o.get_run(&alice(), &only_t2.items[0].run_id).await.unwrap();
    assert_eq!(run, only_t2.items[0]);
    assert_eq!(
        o.get_run(&bob(), &run.run_id).await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
    assert_eq!(
        o.get_run(&alice(), "nope").await.unwrap_err().code(),
        ErrorCode::NotFound
    );

    // History search across sessions, newest first, with filters.
    let hits = o
        .search_history(
            &alice(),
            &HistorySearch::new("apples"),
            PageRequest::first(10),
        )
        .await
        .unwrap();
    assert_eq!(hits.items.len(), 2);
    let all = {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let page = o
                .search_history(
                    &alice(),
                    &HistorySearch::new("apples"),
                    PageRequest {
                        cursor: cursor.clone(),
                        limit: 10,
                    },
                )
                .await
                .unwrap();
            out.extend(page.items);
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break out,
            }
        }
    };
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].session_id, s2, "newest first");
    assert!(all.iter().all(|h| h.root_session_id == h.session_id));
    assert!(all
        .iter()
        .all(|h| h.snippet.to_lowercase().contains("apples")));

    let only_s1_user = o
        .search_history(
            &alice(),
            &HistorySearch {
                text: "apples".into(),
                root_session_id: Some(s1.clone()),
                role: Some(MessageRole::User),
            },
            PageRequest::first(10),
        )
        .await
        .unwrap();
    assert_eq!(only_s1_user.items.len(), 1);
    assert_eq!(
        only_s1_user.items[0].message.text,
        "question two about Apples"
    );

    assert!(o
        .search_history(
            &bob(),
            &HistorySearch::new("apples"),
            PageRequest::first(10)
        )
        .await
        .unwrap()
        .items
        .is_empty());
    assert!(o
        .search_history(
            &bob(),
            &HistorySearch {
                root_session_id: Some(s1),
                ..HistorySearch::new("apples")
            },
            PageRequest::first(10)
        )
        .await
        .is_err());
}
