//! Shared ClickHouse HTTP client used by the three signal-store
//! backends. Wraps `reqwest` with: base URL, database name, optional
//! basic auth, and helpers for the three operations the stores need —
//! `execute` (DDL), `insert_json` (bulk `JSONEachRow`), and
//! `query_json` (single-result-set SELECTs).
//!
//! Mirrors the pattern used by `clickhouse_error_events.rs` but is
//! reusable across the LogStore/EventStore/MetricStore impls.

use crate::core::error::{Error, Result};
use reqwest::Client;
use serde::Serialize;
use std::time::Duration;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct ClickHouseClient {
    base: String, // http://host:port
    db: String,
    user: Option<String>,
    pass: Option<String>,
    http: Client,
}

impl ClickHouseClient {
    pub fn new(
        host: &str,
        port: u16,
        db: &str,
        user: Option<String>,
        pass: Option<String>,
        http_timeout: Duration,
    ) -> Result<Self> {
        let http = Client::builder()
            .timeout(http_timeout)
            .tcp_nodelay(true)
            .pool_max_idle_per_host(16)
            .build()
            .map_err(|e| Error::Internal(format!("clickhouse http client: {e}")))?;
        Ok(Self {
            base: format!("http://{host}:{port}"),
            db: db.to_string(),
            user,
            pass,
            http,
        })
    }

    pub fn database(&self) -> &str {
        &self.db
    }

    fn auth_post(&self, url: String) -> reqwest::RequestBuilder {
        let mut req = self.http.post(url);
        if let (Some(u), Some(p)) = (self.user.as_ref(), self.pass.as_ref()) {
            req = req.basic_auth(u, Some(p));
        }
        req
    }

    /// Execute a DDL or otherwise side-effecting SQL statement.
    /// Used for `CREATE TABLE`, `ALTER`, etc.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        let url = format!("{}/?query={}", self.base, urlencoding::encode(sql));
        // ClickHouse requires either Content-Length or
        // Transfer-Encoding: chunked on POST, even for an empty
        // body — sending an explicit empty body sets Content-Length:0.
        let resp = self
            .auth_post(url)
            .header("Content-Length", "0")
            .send()
            .await
            .map_err(|e| Error::Internal(format!("clickhouse execute: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Internal(format!(
                "clickhouse execute failed ({status}): {body}"
            )));
        }
        Ok(())
    }

    /// Bulk insert via `JSONEachRow`. Each row is serialized to one
    /// JSON object on its own line. `table` is the unqualified
    /// table name; the client prepends `{database}.`.
    pub async fn insert_json<T: Serialize>(&self, table: &str, rows: &[T]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = String::with_capacity(rows.len() * 256);
        for row in rows {
            let line = serde_json::to_string(row)
                .map_err(|e| Error::Internal(format!("clickhouse serialize row: {e}")))?;
            body.push_str(&line);
            body.push('\n');
        }
        let query = format!("INSERT INTO {}.{} FORMAT JSONEachRow", self.db, table);
        let url = format!("{}/?query={}", self.base, urlencoding::encode(&query));
        let resp = self
            .auth_post(url)
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("clickhouse insert: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Internal(format!(
                "clickhouse insert failed ({status}): {body}"
            )));
        }
        Ok(())
    }

    /// Run a SELECT and deserialize each row from `JSONEachRow`.
    pub async fn query_json<T: serde::de::DeserializeOwned>(
        &self,
        sql: &str,
    ) -> Result<Vec<T>> {
        // Append `FORMAT JSONEachRow` so the server emits one object
        // per line (easy line-split parse).
        let full_sql = if sql.to_uppercase().contains("FORMAT") {
            sql.to_string()
        } else {
            format!("{sql} FORMAT JSONEachRow")
        };
        // `output_format_json_quote_64bit_integers=0` keeps UInt64
        // values as plain JSON numbers — by default ClickHouse wraps
        // 64-bit ints in quotes for JS precision-preservation, which
        // our serde u64 fields can't deserialize.
        let url = format!(
            "{}/?query={}&output_format_json_quote_64bit_integers=0",
            self.base,
            urlencoding::encode(&full_sql),
        );
        let resp = self
            .auth_post(url)
            .header("Content-Length", "0")
            .send()
            .await
            .map_err(|e| Error::Internal(format!("clickhouse query: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Internal(format!(
                "clickhouse query failed ({status}): {body}"
            )));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Internal(format!("clickhouse read body: {e}")))?;

        let mut out = Vec::new();
        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<T>(line) {
                Ok(row) => out.push(row),
                Err(e) => warn!("clickhouse row parse: {e} (line: {})", line.chars().take(120).collect::<String>()),
            }
        }
        Ok(out)
    }

    /// Single-row scalar query — returns the first cell of the
    /// first row, or 0 on empty.
    pub async fn query_count(&self, sql: &str) -> Result<u64> {
        let rows: Vec<serde_json::Value> = self.query_json(sql).await?;
        if let Some(first) = rows.first() {
            // ClickHouse returns counts as numbers or string-numbers
            // depending on the type. Cover both.
            if let Some(n) = first.as_object().and_then(|o| o.values().next()) {
                if let Some(v) = n.as_u64() {
                    return Ok(v);
                }
                if let Some(s) = n.as_str() {
                    if let Ok(v) = s.parse::<u64>() {
                        return Ok(v);
                    }
                }
            }
        }
        Ok(0)
    }
}
