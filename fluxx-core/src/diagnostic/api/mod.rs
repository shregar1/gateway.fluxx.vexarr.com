//! Diagnostic API Layer
//!
//! HTTP API for diagnostic operations.

use crate::diagnostic::{
    DiagnosticEngine,
    models::{DiagnosticRequest, DiagnosticFeedback, ErrorContext},
};
use axum::{
    extract::{State, Json, Path},
    http::StatusCode,
    response::Json as JsonResponse,
    routing::{get, post},
    Router,
};
use std::sync::Arc;

/// API state
#[derive(Clone)]
pub struct ApiState {
    pub engine: Arc<DiagnosticEngine>,
}

/// Create diagnostic API routes
pub fn routes(engine: Arc<DiagnosticEngine>) -> Router {
    let state = ApiState { engine };

    Router::new()
        .route("/diagnose", post(diagnose))
        .route("/diagnose/alert/:alert_id", post(diagnose_alert))
        .route("/reports/:report_id", get(get_report))
        .route("/reports/:report_id/feedback", post(submit_feedback))
        .route("/reports/:report_id/ticket", post(create_ticket))
        .route("/health", get(health_check))
        .route("/knowledge-base", get(search_knowledge_base))
        .route("/remediate/:report_id", post(apply_remediation))
        .with_state(state)
}

/// Diagnose an error
async fn diagnose(
    State(state): State<ApiState>,
    Json(request): Json<DiagnoseRequest>,
) -> Result<JsonResponse<DiagnoseResponse>, StatusCode> {
    let context = request.context.unwrap_or_default();
    let diagnostic_request = DiagnosticRequest {
        error: ErrorContext {
            message: request.message,
            error_type: request.error_type.unwrap_or_else(|| "Unknown".to_string()),
            source: request.source.unwrap_or_else(|| "unknown".to_string()),
            severity: request.severity.unwrap_or_else(|| "warning".to_string()),
            context: context.clone(),
            ..Default::default()
        },
        request_id: request.request_id,
        context,
        preferred_language: request.preferred_language,
        include_remediation: request.include_remediation.unwrap_or(true),
    };

    match state.engine.diagnose(diagnostic_request).await {
        Ok(response) => Ok(JsonResponse(DiagnoseResponse {
            report_id: response.report.id,
            root_cause: response.report.root_cause.primary_cause,
            explanation: response.report.root_cause.explanation,
            fixes: response.report.suggested_fixes.into_iter().map(|f| FixResponse {
                id: f.id,
                title: f.title,
                description: f.description,
                confidence: f.confidence,
                risk_level: f.risk_level.as_str().to_string(),
                auto_applicable: f.auto_applicable,
            }).collect(),
            code_samples: response.report.code_samples.into_iter().map(|s| CodeSampleResponse {
                title: s.title,
                language: s.language,
                code: s.code,
                explanation: s.explanation,
            }).collect(),
            ticket: response.ticket.map(|t| TicketResponse {
                id: t.id,
                url: t.url,
                provider: t.provider,
            }),
            next_steps: response.next_steps,
            confidence: response.report.confidence,
            processing_time_ms: response.report.processing_time_ms,
        })),
        Err(e) => {
            tracing::error!("Diagnosis failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Diagnose from alert ID
async fn diagnose_alert(
    State(_state): State<ApiState>,
    Path(_alert_id): Path<String>,
) -> Result<JsonResponse<DiagnoseResponse>, StatusCode> {
    // Would fetch alert by ID and diagnose
    Err(StatusCode::NOT_IMPLEMENTED)
}

/// Get diagnostic report
async fn get_report(
    State(_state): State<ApiState>,
    Path(_report_id): Path<String>,
) -> Result<JsonResponse<serde_json::Value>, StatusCode> {
    // Would retrieve report from storage
    Err(StatusCode::NOT_IMPLEMENTED)
}

/// Submit feedback
async fn submit_feedback(
    State(state): State<ApiState>,
    Path(report_id): Path<String>,
    Json(request): Json<FeedbackRequest>,
) -> Result<StatusCode, StatusCode> {
    let feedback = DiagnosticFeedback {
        report_id,
        helpful: request.helpful,
        fix_successful: request.fix_successful,
        correct_root_cause: request.correct_root_cause,
        comments: request.comments,
        applied_fix_id: request.applied_fix_id,
    };

    match state.engine.submit_feedback(feedback).await {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => {
            tracing::error!("Failed to submit feedback: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Create ticket for report
async fn create_ticket(
    State(state): State<ApiState>,
    Path(_report_id): Path<String>,
) -> Result<JsonResponse<TicketResponse>, StatusCode> {
    // Would retrieve report and create ticket
    match state.engine.health_check().await {
        Ok(()) => Ok(JsonResponse(TicketResponse {
            id: "test-123".to_string(),
            url: "https://example.com/ticket/123".to_string(),
            provider: "jira".to_string(),
        })),
        Err(_) => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}

/// Health check
async fn health_check(State(state): State<ApiState>) -> Result<JsonResponse<HealthResponse>, StatusCode> {
    match state.engine.health_check().await {
        Ok(()) => Ok(JsonResponse(HealthResponse {
            status: "healthy".to_string(),
            ai_enabled: state.engine.analyzer.is_some(),
            ticketing_enabled: !state.engine.ticketing_providers.is_empty(),
        })),
        Err(e) => {
            tracing::warn!("Health check failed: {}", e);
            Ok(JsonResponse(HealthResponse {
                status: "degraded".to_string(),
                ai_enabled: state.engine.analyzer.is_some(),
                ticketing_enabled: !state.engine.ticketing_providers.is_empty(),
            }))
        }
    }
}

/// Search knowledge base
async fn search_knowledge_base(
    State(state): State<ApiState>,
) -> Result<JsonResponse<Vec<KnowledgeArticleResponse>>, StatusCode> {
    let kb_arc = state.engine.knowledge_base();
    let kb = kb_arc.read().await;
    
    let articles: Vec<KnowledgeArticleResponse> = kb.articles.iter().map(|a| KnowledgeArticleResponse {
        id: a.id.clone(),
        title: a.title.clone(),
        similarity: a.similarity,
        tags: a.tags.clone(),
    }).collect();

    Ok(JsonResponse(articles))
}

/// Apply remediation
async fn apply_remediation(
    State(_state): State<ApiState>,
    Path(_report_id): Path<String>,
    Json(_request): Json<RemediateRequest>,
) -> Result<JsonResponse<RemediateResponse>, StatusCode> {
    // Would apply the remediation plan
    Err(StatusCode::NOT_IMPLEMENTED)
}

// Request/Response types

#[derive(serde::Deserialize)]
struct DiagnoseRequest {
    message: String,
    error_type: Option<String>,
    source: Option<String>,
    severity: Option<String>,
    context: Option<std::collections::HashMap<String, String>>,
    request_id: Option<String>,
    preferred_language: Option<String>,
    include_remediation: Option<bool>,
}

#[derive(serde::Serialize)]
struct DiagnoseResponse {
    report_id: String,
    root_cause: String,
    explanation: String,
    fixes: Vec<FixResponse>,
    code_samples: Vec<CodeSampleResponse>,
    ticket: Option<TicketResponse>,
    next_steps: Vec<String>,
    confidence: f64,
    processing_time_ms: u64,
}

#[derive(serde::Serialize)]
struct FixResponse {
    id: String,
    title: String,
    description: String,
    confidence: f64,
    risk_level: String,
    auto_applicable: bool,
}

#[derive(serde::Serialize)]
struct CodeSampleResponse {
    title: String,
    language: String,
    code: String,
    explanation: String,
}

#[derive(serde::Serialize)]
struct TicketResponse {
    id: String,
    url: String,
    provider: String,
}

#[derive(serde::Deserialize)]
struct FeedbackRequest {
    helpful: bool,
    fix_successful: Option<bool>,
    correct_root_cause: Option<String>,
    comments: Option<String>,
    applied_fix_id: Option<String>,
}

#[derive(serde::Serialize)]
struct HealthResponse {
    status: String,
    ai_enabled: bool,
    ticketing_enabled: bool,
}

#[derive(serde::Serialize)]
struct KnowledgeArticleResponse {
    id: String,
    title: String,
    similarity: f64,
    tags: Vec<String>,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct RemediateRequest {
    fix_id: Option<String>,
    auto_approve: Option<bool>,
}

#[derive(serde::Serialize)]
struct RemediateResponse {
    success: bool,
    message: String,
    applied_fixes: Vec<String>,
}
