//! Notebooks — Collaborative investigation documents with embedded queries.
//! Like Datadog Notebooks: mix markdown, charts, and live queries in one document.

use crate::api::{ApiState, StandardResponse};
use axum::{extract::{State, Path}, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notebook {
    pub id: String,
    pub title: String,
    pub author: String,
    pub created_at_us: u64,
    pub updated_at_us: u64,
    pub cells: Vec<NotebookCell>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub shared: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotebookCell {
    Markdown { content: String },
    Query { query: String, visualization: String },
    Metric { metric_name: String, time_range: String },
    LogSearch { query: String, service: Option<String> },
    TraceView { trace_id: String },
    Image { url: String, caption: Option<String> },
}

fn now_us() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as u64 }

#[derive(Debug, Default)]
pub struct NotebookStore {
    notebooks: RwLock<HashMap<String, Notebook>>,
}

impl NotebookStore {
    pub fn new() -> Self { Self::default() }
    pub fn create(&self, mut nb: Notebook) -> Notebook {
        if nb.id.is_empty() { nb.id = uuid::Uuid::new_v4().to_string(); }
        nb.created_at_us = now_us(); nb.updated_at_us = nb.created_at_us;
        self.notebooks.write().insert(nb.id.clone(), nb.clone()); nb
    }
    pub fn update(&self, id: &str, mut nb: Notebook) -> Option<Notebook> {
        let mut store = self.notebooks.write();
        if store.contains_key(id) { nb.id = id.to_string(); nb.updated_at_us = now_us(); store.insert(id.to_string(), nb.clone()); Some(nb) } else { None }
    }
    pub fn list(&self) -> Vec<Notebook> {
        let store = self.notebooks.read();
        let mut r: Vec<Notebook> = store.values().cloned().collect();
        r.sort_by(|a, b| b.updated_at_us.cmp(&a.updated_at_us)); r
    }
    pub fn get(&self, id: &str) -> Option<Notebook> { self.notebooks.read().get(id).cloned() }
    pub fn delete(&self, id: &str) -> bool { self.notebooks.write().remove(id).is_some() }
}

pub async fn create_notebook(State(state): State<Arc<ApiState>>, Json(nb): Json<Notebook>) -> impl IntoResponse {
    let created = state.notebook_store.create(nb);
    StandardResponse::ok(serde_json::to_value(&created).unwrap_or_default(), "Notebook created.")
}
pub async fn list_notebooks(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    StandardResponse::ok(serde_json::to_value(&state.notebook_store.list()).unwrap_or_default(), "Notebooks.")
}
pub async fn get_notebook(Path(id): Path<String>, State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    match state.notebook_store.get(&id) {
        Some(nb) => StandardResponse::ok(serde_json::to_value(&nb).unwrap_or_default(), "Notebook."),
        None => StandardResponse::error(serde_json::json!({}), "Not found", "NOT_FOUND", vec![]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_notebook(title: &str) -> Notebook {
        Notebook {
            id: String::new(),
            title: title.to_string(),
            author: "alice".to_string(),
            created_at_us: 0,
            updated_at_us: 0,
            cells: vec![],
            tags: vec![],
            shared: false,
        }
    }

    #[test]
    fn create_assigns_id_when_empty() {
        let store = NotebookStore::new();
        let nb = store.create(mk_notebook("first"));
        assert!(!nb.id.is_empty());
    }

    #[test]
    fn create_preserves_provided_id() {
        let store = NotebookStore::new();
        let mut nb = mk_notebook("keep-id");
        nb.id = "custom-id".to_string();
        let created = store.create(nb);
        assert_eq!(created.id, "custom-id");
    }

    #[test]
    fn create_sets_timestamps() {
        let store = NotebookStore::new();
        let nb = store.create(mk_notebook("t"));
        assert!(nb.created_at_us > 0);
        assert_eq!(nb.created_at_us, nb.updated_at_us);
    }

    #[test]
    fn get_returns_none_for_missing() {
        let store = NotebookStore::new();
        assert!(store.get("missing").is_none());
    }

    #[test]
    fn get_returns_created_notebook() {
        let store = NotebookStore::new();
        let nb = store.create(mk_notebook("hello"));
        let got = store.get(&nb.id).expect("should exist");
        assert_eq!(got.title, "hello");
    }

    #[test]
    fn update_existing_returns_some() {
        let store = NotebookStore::new();
        let nb = store.create(mk_notebook("old"));
        let mut updated = nb.clone();
        updated.title = "new".to_string();
        let result = store.update(&nb.id, updated).expect("should update");
        assert_eq!(result.title, "new");
        assert_eq!(result.id, nb.id);
    }

    #[test]
    fn update_missing_returns_none() {
        let store = NotebookStore::new();
        assert!(store.update("nope", mk_notebook("x")).is_none());
    }

    #[test]
    fn update_refreshes_updated_at() {
        let store = NotebookStore::new();
        let nb = store.create(mk_notebook("x"));
        std::thread::sleep(std::time::Duration::from_millis(2));
        let updated = store.update(&nb.id, nb.clone()).unwrap();
        assert!(updated.updated_at_us >= nb.updated_at_us);
    }

    #[test]
    fn delete_existing_returns_true() {
        let store = NotebookStore::new();
        let nb = store.create(mk_notebook("gone"));
        assert!(store.delete(&nb.id));
        assert!(store.get(&nb.id).is_none());
    }

    #[test]
    fn delete_missing_returns_false() {
        let store = NotebookStore::new();
        assert!(!store.delete("nope"));
    }

    #[test]
    fn list_empty_returns_empty_vec() {
        let store = NotebookStore::new();
        assert!(store.list().is_empty());
    }

    #[test]
    fn list_sorts_by_updated_at_desc() {
        let store = NotebookStore::new();
        let a = store.create(mk_notebook("a"));
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = store.create(mk_notebook("b"));
        let list = store.list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].id, b.id);
        assert_eq!(list[1].id, a.id);
    }

    #[test]
    fn list_returns_all_created() {
        let store = NotebookStore::new();
        for i in 0..5 {
            store.create(mk_notebook(&format!("nb-{}", i)));
        }
        assert_eq!(store.list().len(), 5);
    }

    #[test]
    fn unique_ids_when_auto_generated() {
        let store = NotebookStore::new();
        let a = store.create(mk_notebook("a"));
        let b = store.create(mk_notebook("b"));
        assert_ne!(a.id, b.id);
    }
}
