use super::*;
use serde_json::json;

#[test]
fn chunks_respect_char_boundaries_and_order() {
    let parts = chunks("aé😀bcd", 4);
    assert_eq!(parts.concat(), "aé😀bcd");
    assert!(parts.iter().all(|p| p.len() <= 4));
}

#[test]
fn delta_filter_redacts_secret_split_across_fragments() {
    let secret = "sk-secret-value-123";
    let mut f = DeltaFilter::new(vec![secret.to_owned()]);
    let mut out = String::new();
    for frag in ["key is sk-sec", "ret-val", "ue-123 ok ", "bye"] {
        out.push_str(&f.push(frag));
    }
    out.push_str(&f.flush());
    assert_eq!(out, "key is [redacted] ok bye");
    assert!(!out.contains("secret"));
}

#[test]
fn delta_filter_is_transparent_without_secrets() {
    let mut f = DeltaFilter::new(Vec::new());
    assert_eq!(f.push("hello"), "hello");
    assert_eq!(f.flush(), "");
}

#[test]
fn short_secrets_are_not_redacted() {
    let feed = ExecutionFeed::new(8, 64, vec!["abc".into()]);
    assert_eq!(feed.redact("abc"), "abc");
}

#[tokio::test]
async fn lagged_subscriber_resyncs_and_ends_without_blocking_publisher() {
    let feed = ExecutionFeed::new(2, 64, Vec::new());
    let mut slow = feed.subscribe("root");
    for i in 0..10 {
        feed.ephemeral("root", "root", None, "text.delta", json!({ "i": i }));
    }
    assert_eq!(slow.recv().await, Some(FeedItem::ResyncRequired));
    assert_eq!(slow.recv().await, None);
}

#[tokio::test]
async fn subscription_only_sees_its_own_family() {
    let feed = ExecutionFeed::new(8, 64, Vec::new());
    let mut a = feed.subscribe("a");
    feed.ephemeral("b", "b", None, "text.delta", json!({}));
    feed.ephemeral("a", "a", None, "text.delta", json!({}));
    match a.recv().await {
        Some(FeedItem::Event(e)) => assert_eq!(e.root_session_id, "a"),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn closing_the_feed_drains_queued_events_then_ends() {
    let feed = ExecutionFeed::new(8, 64, Vec::new());
    let mut a = feed.subscribe("a");
    feed.ephemeral("a", "a", None, "text.delta", json!({}));
    feed.close();
    assert!(matches!(a.recv().await, Some(FeedItem::Event(_))));
    assert_eq!(a.recv().await, None);
}
