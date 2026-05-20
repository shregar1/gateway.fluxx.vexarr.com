//! Integration tests for the ClickHouse-backed
//! LogStore / EventStore / MetricStore.
//!
//! Gated behind `#[ignore]` — they each spin a real ClickHouse
//! container via `testcontainers`, which needs Docker running.
//! Run with:
//!
//!     cargo test -p fluxx-core --test clickhouse_signals_integration -- --ignored --nocapture
//!
//! The tests share container shape so we keep one helper and three
//! thin test bodies. Each test waits a beat after insert for the
//! batch drainer to flush before asserting on count.

#![cfg(test)]

use fluxx_core::models::event::{Event, ProtocolType};
use fluxx_core::models::log_entry::{LogEntry, LogLevel};
use fluxx_core::models::metric::{Metric, MetricType};
use fluxx_core::storage::clickhouse_client::ClickHouseClient;
use fluxx_core::storage::event_store::{EventStore, TimeWindow};
use fluxx_core::storage::log_store::{LogQuery, LogStore};
use fluxx_core::storage::metric_store::MetricStore;
use fluxx_core::storage::{
    ClickHouseEventStore, ClickHouseLogStore, ClickHouseMetricStore,
};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::{
    core::{wait::HttpWaitStrategy, ContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

const CH_IMAGE: &str = "clickhouse/clickhouse-server";
const CH_TAG: &str = "24.8";
const CH_HTTP_PORT: u16 = 8123;
const DATABASE: &str = "fluxx_test";

/// Spin one ClickHouse container, return (container handle, mapped
/// host port). Caller keeps the container alive; on drop it's torn
/// down by Docker.
async fn boot_clickhouse() -> (ContainerAsync<GenericImage>, u16) {
    // ClickHouse's HTTP `/ping` returns 200 once the server is up
    // and the database is ready — works regardless of CH version's
    // log message format (the JSON logger doesn't emit the classic
    // "Ready for connections" stderr line).
    let wait = HttpWaitStrategy::new("/ping")
        .with_port(ContainerPort::Tcp(CH_HTTP_PORT))
        .with_expected_status_code(200u16)
        .with_poll_interval(Duration::from_millis(500));
    let container = GenericImage::new(CH_IMAGE, CH_TAG)
        .with_exposed_port(ContainerPort::Tcp(CH_HTTP_PORT))
        .with_wait_for(WaitFor::http(wait))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_env_var("CLICKHOUSE_DB", DATABASE)
        .start()
        .await
        .expect("clickhouse container start");
    let port = container
        .get_host_port_ipv4(CH_HTTP_PORT)
        .await
        .expect("clickhouse host port");
    (container, port)
}

fn make_client(port: u16) -> Arc<ClickHouseClient> {
    Arc::new(
        ClickHouseClient::new(
            "127.0.0.1",
            port,
            DATABASE,
            None,
            None,
            Duration::from_secs(10),
        )
        .expect("clickhouse client"),
    )
}

/// Helper: poll `query` for up to `max_wait` until it returns a
/// non-zero result. ClickHouse async batching means we can't rely
/// on an immediate-visible insert.
async fn wait_for<F, Fut>(max_wait: Duration, mut probe: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + max_wait;
    while std::time::Instant::now() < deadline {
        if probe().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn clickhouse_log_store_roundtrip() {
    let (_container, port) = boot_clickhouse().await;
    let client = make_client(port);
    let store = ClickHouseLogStore::new(client, 1000, 100, Duration::from_millis(200))
        .await
        .expect("create clickhouse log store");

    for i in 0..5 {
        store
            .insert(LogEntry::new(LogLevel::Info, format!("hello {i}")))
            .await
            .unwrap();
    }

    let ok = wait_for(Duration::from_secs(5), || async {
        store.len().await.unwrap_or(0) >= 5
    })
    .await;
    assert!(ok, "expected 5 rows visible within 5s; got {}", store.len().await.unwrap_or(0));

    let rows = store.query(LogQuery::default()).await.expect("query");
    assert!(rows.iter().any(|r| r.message == "hello 0"));
    assert!(rows.iter().any(|r| r.message == "hello 4"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn clickhouse_event_store_roundtrip() {
    let (_container, port) = boot_clickhouse().await;
    let client = make_client(port);
    let store = ClickHouseEventStore::new(client, 1000, 100, Duration::from_millis(200))
        .await
        .expect("create clickhouse event store");

    for i in 0..3 {
        let mut e = Event::new(format!("ev-{i}"), ProtocolType::Otlp);
        e.timestamp_us = 1_000 + i;
        store.insert(e).await.unwrap();
    }

    let ok = wait_for(Duration::from_secs(5), || async {
        store.len().await.unwrap_or(0) >= 3
    })
    .await;
    assert!(ok, "expected 3 events visible within 5s");

    let all = store.query(TimeWindow::new(0, u64::MAX)).await.expect("query");
    assert_eq!(all.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn clickhouse_metric_store_roundtrip() {
    let (_container, port) = boot_clickhouse().await;
    let client = make_client(port);
    let store = ClickHouseMetricStore::new(client, 1000, 100, Duration::from_millis(200))
        .await
        .expect("create clickhouse metric store");

    for i in 0..4 {
        store
            .insert(
                Metric::new("cpu".to_string(), i as f64, MetricType::Gauge)
                    .with_timestamp(1_000_000 + i * 1_000),
            )
            .await
            .unwrap();
    }

    let ok = wait_for(Duration::from_secs(5), || async {
        store.len().await.unwrap_or(0) >= 4
    })
    .await;
    assert!(ok, "expected 4 metrics visible within 5s");

    let names = store.get_names().await.expect("names");
    assert_eq!(names, vec!["cpu"]);
}
