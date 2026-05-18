# Fluxx Agent Diagnostic & Remediation System

AI-powered error analysis, automated remediation planning, and ticketing integration.

## Features

### 🤖 AI-Powered Analysis
- **Root Cause Analysis**: Automatically identify the underlying cause of errors
- **Suggested Fixes**: Get AI-generated fix recommendations with confidence scores
- **Code Generation**: Receive code samples in your preferred language
- **Remediation Planning**: Step-by-step action plans to resolve issues

### 🎫 Ticketing Integrations
- **Jira**: Auto-create issues with full diagnostic context
- **ServiceNow Issues**: Create issues with formatted markdown
- **Linear**: Create issues via GraphQL API
- **Custom Webhooks**: Send diagnostics to any endpoint

### 🔧 Auto-Remediation
- **Safe Execution**: Apply fixes automatically with confidence thresholds
- **Rollback Support**: Automatic rollback on failure
- **Approval Workflows**: Require human approval for critical changes
- **Step-by-Step Plans**: Structured remediation with verification

### 📚 Knowledge Base
- **Similar Issue Detection**: Find related past issues
- **Continuous Learning**: Improve from user feedback
- **Vector Search**: Semantic similarity matching

## Quick Start

### Configuration

```yaml
# diagnostic.yaml
diagnostic:
  enabled: true
  
  ai:
    provider: openai
    api_key: "${OPENAI_API_KEY}"
    model: gpt-4
    max_tokens: 2000
    
  remediation:
    enabled: true
    auto_apply: false
    confidence_threshold: 0.85
    
  ticketing:
    jira:
      enabled: true
      host: "https://company.atlassian.net"
      username: "${JIRA_USER}"
      api_token: "${JIRA_TOKEN}"
      project_key: "OPS"
      auto_create: true
```

### Usage

```rust
use fluxx_agent::diagnostic::{
    DiagnosticEngine, DiagnosticRequest,
    models::ErrorContext,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize engine
    let engine = DiagnosticEngine::from_env().await?;
    
    // Create diagnostic request
    let request = DiagnosticRequest {
        error: ErrorContext {
            message: "Connection refused".to_string(),
            error_type: "ConnectionError".to_string(),
            source: "api-service".to_string(),
            severity: "critical".to_string(),
            ..Default::default()
        },
        include_remediation: true,
        ..Default::default()
    };
    
    // Run diagnosis
    let response = engine.diagnose(request).await?;
    
    println!("Root cause: {}", response.report.root_cause.primary_cause);
    println!("Fixes: {}", response.report.suggested_fixes.len());
    
    Ok(())
}
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/diagnose` | POST | Submit error for analysis |
| `/reports/:id` | GET | Get diagnostic report |
| `/reports/:id/feedback` | POST | Submit feedback |
| `/reports/:id/ticket` | POST | Create ticket |
| `/knowledge-base` | GET | Search knowledge base |
| `/health` | GET | Health check |

### Diagnose Endpoint

```bash
curl -X POST http://localhost:8080/diagnose \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Connection refused",
    "error_type": "ConnectionError",
    "source": "api-service",
    "severity": "critical",
    "include_remediation": true
  }'
```

Response:
```json
{
  "report_id": "diag-abc123",
  "root_cause": "Database connection pool exhausted",
  "explanation": "Too many concurrent connections to PostgreSQL",
  "confidence": 0.95,
  "fixes": [
    {
      "id": "fix-1",
      "title": "Increase connection pool size",
      "confidence": 0.92,
      "risk_level": "low"
    }
  ],
  "code_samples": [
    {
      "title": "Connection pool configuration",
      "language": "rust",
      "code": "// Code sample here",
      "explanation": "Explanation of the fix"
    }
  ],
  "ticket": {
    "id": "OPS-123",
    "url": "https://jira/OPS-123",
    "provider": "jira"
  },
  "next_steps": [
    "1. Review the root cause: Database connection pool exhausted",
    "2. Review 1 suggested fix(es)..."
  ]
}
```

## AI Providers

### OpenAI
```yaml
ai:
  provider: openai
  api_key: "${OPENAI_API_KEY}"
  model: gpt-4
```

### Anthropic (Claude)
```yaml
ai:
  provider: anthropic
  api_key: "${ANTHROPIC_API_KEY}"
  model: claude-3-opus
```

## Ticketing Configuration

### Jira
```yaml
ticketing:
  jira:
    enabled: true
    host: "https://company.atlassian.net"
    username: "${JIRA_USER}"
    api_token: "${JIRA_TOKEN}"
    project_key: "OPS"
    issue_type: "Bug"
    auto_create: true
    labels: ["auto-generated", "diagnostic"]
```

### ServiceNow
```yaml
ticketing:
  servicenow:
    enabled: true
    token: "${GITHUB_TOKEN}"
    owner: "myorg"
    repo: "myrepo"
    auto_create: true
    labels: ["bug", "auto-generated"]
```

### Linear
```yaml
ticketing:
  linear:
    enabled: true
    api_key: "${LINEAR_API_KEY}"
    team_id: "TEAM_ID"
    auto_create: true
```

## Remediation

### Auto-Remediation
Configure automatic fix application:

```yaml
remediation:
  enabled: true
  auto_apply: true
  confidence_threshold: 0.90
  max_retries: 3
  rollback_on_failure: true
```

### Manual Approval
Require approval for critical fixes:

```yaml
remediation:
  auto_apply: false
  approval_required_for: ["critical", "high"]
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Diagnostic Engine                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   AI         │───▶│  Remediation │───▶│  Ticketing   │  │
│  │  Analyzer    │    │   Engine     │    │  Providers   │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                   │           │
│         ▼                   ▼                   ▼           │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Knowledge Base                          │  │
│  │  (Vector search, Similar issues, Learning)          │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Testing

```bash
# Run diagnostic tests
cargo test diagnostic --lib

# Run with example
cargo run --example diagnostic_example
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | OpenAI API key |
| `ANTHROPIC_API_KEY` | Anthropic API key |
| `JIRA_HOST` | Jira instance URL |
| `JIRA_USER` | Jira username |
| `JIRA_TOKEN` | Jira API token |
| `GITHUB_TOKEN` | ServiceNow personal access token |
| `LINEAR_API_KEY` | Linear API key |

## Feedback & Learning

Submit feedback to improve future diagnostics:

```rust
let feedback = DiagnosticFeedback {
    report_id: report.id.clone(),
    helpful: true,
    fix_successful: Some(true),
    correct_root_cause: None,
    comments: Some("The fix worked perfectly".to_string()),
    applied_fix_id: Some(fix.id.clone()),
};

engine.submit_feedback(feedback).await?;
```

## Security Considerations

- API keys are never logged
- Code changes require approval by default
- Commands are sandboxed
- Rollback is automatic on failure
- Sensitive data is masked in tickets

## Future Enhancements

- [ ] Local LLM support (llama.cpp, Ollama)
- [ ] Multi-modal analysis (logs + metrics + traces)
- [ ] A/B testing for fixes
- [ ] Integration with Runbooks
- [ ] Slack bot for interactive diagnostics
