pub mod dev;
pub mod mesh;
pub mod banner;
pub mod verify;
pub mod init;
pub mod register;
pub mod errors_cmd;
pub mod release_cmd;

use std::path::{Path, PathBuf};

/// Validate and sanitize a user-provided path to prevent path traversal attacks
fn sanitize_path(base_dir: &str, user_path: &str) -> Result<PathBuf, String> {
    // Check for path traversal attempts
    if user_path.contains("..") || user_path.contains("~") {
        return Err("Path contains forbidden characters".to_string());
    }
    
    // Normalize the base directory
    let base = Path::new(base_dir)
        .canonicalize()
        .map_err(|_| "Invalid base directory".to_string())?;
    
    // Join with user path
    let target = base.join(user_path);
    
    // Verify the target is within the base directory
    let canonical_target = target
        .canonicalize()
        .or_else(|_| {
            // Path doesn't exist yet, check parent
            if let Some(parent) = target.parent() {
                let canonical_parent = parent.canonicalize()?;
                Ok(canonical_parent.join(target.file_name().unwrap_or_default()))
            } else {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid path"))
            }
        })
        .map_err(|_| "Failed to canonicalize path".to_string())?;
    
    if !canonical_target.starts_with(&base) {
        return Err("Path traversal detected: target outside base directory".to_string());
    }
    
    Ok(canonical_target)
}

use clap::{Parser, Subcommand};
use crate::config::Config;
use crate::utils::init_logging;
use crate::core::AgentService;
use crate::cli::dev::DevManager;
use crate::cli::banner::{print_banner, print_compact_banner, SystemInfo, print_shutdown};
use crate::license;
use tracing::info;

/// CLI interface: Parser and Entry Point for the Fluxx agent.
#[derive(Parser)]
#[command(name = "fluxx")]
#[command(version, about = "Fluxx: High-performance observability for the edge.", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Dev mode: Automates local service discovery and live telemetry streaming
    Dev,
    
    /// Initialises and runs the agent service
    Start {
        /// Optional path to any configuration file
        #[arg(short, long)]
        config: Option<String>,
    },
    
    /// Inspects the current state of any specified target
    Inspect {
        /// The identifier for the target to inspect
        target: String,
    },

    /// Returns the current configuration summary
    Config,

    /// Provisions and deploys the agent to a specified fleet or cloud provider
    Deploy {
        /// The cloud or infrastructure provider (aws, azure, gcp, local)
        #[arg(short, long)]
        provider: String,

        /// The region for the provider (AWS specific)
        #[arg(short, long)]
        region: Option<String>,

        /// The deployment environment (e.g. production, staging)
        #[arg(short, long)]
        env: Option<String>,

        /// The IP/Host for a local self-hosted hub
        #[arg(long)]
        hub_ip: Option<String>,

        /// The cluster URI for an Azure Data Explorer hub
        #[arg(long)]
        cluster_uri: Option<String>,

        /// The project ID for a GCP deployment
        #[arg(long)]
        project: Option<String>,

        /// The instance ID for a GCP deployment
        #[arg(long)]
        instance: Option<String>,
    },
    
    /// Mesh networking commands
    Mesh(mesh::MeshArgs),

    /// Starts the MCP (Model Context Protocol) server for AI assistants
    Mcp {
        /// Optional path to configuration
        #[arg(short, long)]
        config: Option<String>,
    },

    /// OpenClaw autonomous agent integration commands
    Openclaw {
        #[command(subcommand)]
        subcommand: OpenclawSubcommands,
    },

    /// Verify that a producer service is emitting OTLP spans to this
    /// agent. Last-mile wiring check, designed as a CI / post-deploy
    /// gate. Exit 0 = at least one fresh span seen, exit 1 otherwise.
    Verify(verify::VerifyArgs),

    /// Initialise a repo as a Fluxx producer. Detects the stack,
    /// writes `fluxx.toml`, and prints the SDK-wiring snippet for
    /// the detected language/framework. Safe to re-run (won't
    /// overwrite an existing fluxx.toml without --force).
    Init(init::InitArgs),

    /// Manually announce a service to Fluxx by sending one synthetic
    /// OTel span to the local agent. The service appears in the
    /// dashboard /service-map within ~30s, before any real production
    /// traffic flows. Useful for pre-warming the dashboard or attaching
    /// owner/tier/description metadata on a low-traffic service.
    Register(register::RegisterArgs),

    /// Query the local agent's error tracker. Reads `/v1/errors/*`
    /// on the running agent and prints a terminal-friendly view —
    /// useful for CI gates ("fluxx errors top -n 10 | grep YourError")
    /// and operator triage.
    Errors(errors_cmd::ErrorsArgs),

    /// Declare application releases for issue / regression tracking.
    /// Calls api.fluxx.dev's `/api/v1/app-releases`. Auth via
    /// FLUXX_API_KEY.
    Release(release_cmd::ReleaseArgs),
}

#[derive(Subcommand)]
pub enum OpenclawSubcommands {
    /// Initialize OpenClaw skills and heartbeats in a target directory
    Init {
        /// Path to the OpenClaw agent workspace or skills directory
        #[arg(short, long, default_value = "./.openclaw")]
        path: String,
    },
    /// Check the status of OpenClaw integration
    Status,
}

pub async fn run() -> anyhow::Result<()> {
    init_logging();
    let cli = Cli::parse();
    // Bootstrap the admin token before we mount the auth middleware.
    // First run mints + prints; subsequent runs silently load the hash.
    // Subcommands that only talk to the API (errors, release, verify)
    // don't need to mint anything, but it's cheap to call regardless.
    let auth_outcome = crate::security::bootstrap_token::init_admin_token();
    if !matches!(cli.command, Commands::Errors(_) | Commands::Release(_) | Commands::Verify(_) | Commands::Init(_) | Commands::Register(_)) {
        info!("Auth bootstrap: {:?}", auth_outcome);
    }
    let mut service = AgentService::new();

    let requires_license = matches!(cli.command, Commands::Dev | Commands::Start { .. } | Commands::Mesh(_));

    // Self-hosted mode: the customer is running Fluxx entirely on their
    // own infrastructure and not talking to api.fluxx.dev. Trigger on
    // either the explicit env flag or the absence of *both* the SaaS
    // URL and an API key — that combination means there's nowhere for
    // the license check to even resolve to, so insisting on it would
    // just block startup with no upside.
    let self_hosted = std::env::var("FLUXX_SELF_HOSTED")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(false)
        || (std::env::var("FLUXX_API_URL").is_err()
            && std::env::var("FLUXX_API_KEY").is_err());

    if requires_license && !self_hosted {
        match license::validate_subscription().await {
            Ok(info) => {
                info!(
                    "License OK: tier={}",
                    info.tier.as_deref().unwrap_or("free")
                );
            }
            Err(msg) if msg == "OFFLINE_OK" => {
                info!("Running in offline mode — license check skipped");
            }
            Err(msg) => {
                license::print_license_error(&msg);
                std::process::exit(1);
            }
        }
    } else if requires_license {
        info!("Self-hosted mode — license check, cloud forwarder, and runtime grants disabled");
    }

    match cli.command {
        Commands::Dev => {
            let info = SystemInfo::new();
            print_banner(&info);

            let mut dev = DevManager::new();
            dev.run().await?;

            print_shutdown();
        }
        Commands::Start { config } => {
            let info = SystemInfo::with_stats(45, 0);
            print_banner(&info);

            info!("CLI: Initializing Start flow...");
            let cfg = match config {
                Some(path) => Config::load_from_path(&path).unwrap_or_default(),
                None => Config::load().unwrap_or_default(),
            };
            service.run_agent(cfg).await?;

            print_shutdown();
        }
        Commands::Inspect { target } => {
            let res = service.inspect_target(&target);
            println!("{}", res);
        }
        Commands::Config => {
            // Show compact banner for config command
            let info = SystemInfo::new();
            print_compact_banner(&info);
            
            let cfg = Config::load().unwrap_or_default();
            let res = service.get_config_summary(&cfg);
            println!("{}", res);
        }
        Commands::Deploy { 
            provider, region, env, hub_ip, cluster_uri, project, instance 
        } => {
            let res = service.deploy_fleet(provider, region, env, hub_ip, cluster_uri, project, instance).await?;
            println!("{}", res);
        }
        Commands::Mesh(args) => {
            // Show compact banner for mesh commands
            let info = SystemInfo::new();
            print_compact_banner(&info);
            
            mesh::handle_command(args)?;
        }
        Commands::Mcp { config } => {
            // Show compact banner for MCP server
            let info = SystemInfo::with_stats(45, 0);
            print_compact_banner(&info);
            
            info!("CLI: Initializing MCP server flow...");
            let cfg = match config {
                Some(_path) => crate::ai::McpConfig::default(), // Future: load from path
                None => crate::ai::McpConfig::default(),
            };
            
            // We need an IntegrationManager for the MCP server
            // In a real scenario, this would be part of the service state
            let manager = std::sync::Arc::new(tokio::sync::RwLock::new(
                crate::integrations::IntegrationManager::new()
            ));
            
            // Run the server
            crate::ai::mcp_server::start_mcp_server_stdio(manager, cfg).await.map_err(|e| anyhow::anyhow!(e))?;
        }
        Commands::Openclaw { subcommand } => {
            match subcommand {
                OpenclawSubcommands::Init { path } => {
                    info!("CLI: Initializing OpenClaw integration at {}...", path);
                    
                    // Sanitize the path to prevent directory traversal
                    let base_dir = std::env::current_dir()
                        .map_err(|e| anyhow::anyhow!("Cannot get current directory: {}", e))?;
                    let safe_path = sanitize_path(
                        base_dir.to_str().unwrap_or("."),
                        &path
                    ).map_err(|e| anyhow::anyhow!("Invalid path: {}", e))?;
                    
                    // Create directories
                    let skills_dir = safe_path.join("skills/fluxx_observability");
                    std::fs::create_dir_all(&skills_dir)?;
                    
                    // Write SKILL.md (simplified version here, ideally read from resources)
                    let skill_content = include_str!("../../extensions/openclaw/fluxx_observability/SKILL.md");
                    std::fs::write(skills_dir.join("SKILL.md"), skill_content)?;
                    
                    // Write HEARTBEAT.md
                    let heartbeat_content = "---\ninterval: 30m\ndescription: Periodic health check of infrastructure via Fluxx Agent\n---\n# Fluxx Heartbeat\n1. Use `fluxx inspect all` to check system health.\n2. If any services are unhealthy, use `fluxx_diagnose` to find the root cause.\n3. Report any critical issues to the user immediately.";
                    std::fs::write(safe_path.join("HEARTBEAT.md"), heartbeat_content)?;
                    
                    println!("✅ OpenClaw integration initialized at {}.", safe_path.display());
                    println!("Fluxx Skill: {}/SKILL.md", skills_dir.display());
                    println!("Heartbeat: {}/HEARTBEAT.md", safe_path.display());
                }
                OpenclawSubcommands::Status => {
                    println!("OpenClaw integration version 0.1.0");
                    println!("Capabilities: [Metrics, Health, Diagnostics, Events]");
                    println!("Status: Active (using MCP stdio transport)");
                }
            }
        }
        Commands::Verify(args) => {
            // No banner / no logging-init noise — `verify` is meant
            // for CI pipelines, where every extra line of stderr is
            // a downstream parsing headache. Just run it.
            verify::run(args).await?;
        }
        Commands::Init(args) => {
            // Same reasoning as verify: this command is meant to be
            // invoked inside a producer repo, sometimes as part of a
            // template or CI workflow, so we skip the ASCII banner.
            init::run(args).await?;
        }
        Commands::Register(args) => {
            // Like verify/init, intended for CI and one-off operator
            // invocations. Skip banner; print only the success line.
            register::run(args).await?;
        }
        Commands::Errors(args) => {
            // No banner — CI / shell tooling pipes the output.
            errors_cmd::run(args).await?;
        }
        Commands::Release(args) => {
            // No banner — invoked from CI post-deploy hooks.
            release_cmd::run(args).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing_start() {
        let cli = Cli::try_parse_from(&["fluxx", "start", "--config", "env.json"]).unwrap();
        match cli.command {
            Commands::Start { config } => assert_eq!(config, Some("env.json".to_string())),
            _ => panic!("Expected Start command"),
        }
    }

    #[test]
    fn test_cli_parsing_inspect() {
        let cli = Cli::try_parse_from(&["fluxx", "inspect", "http"]).unwrap();
        match cli.command {
            Commands::Inspect { target } => assert_eq!(target, "http"),
            _ => panic!("Expected Inspect command"),
        }
    }

    #[test]
    fn test_cli_parsing_config() {
        let cli = Cli::try_parse_from(&["fluxx", "config"]).unwrap();
        match cli.command {
            Commands::Config => {},
            _ => panic!("Expected Config command"),
        }
    }

    #[test]
    fn test_cli_parsing_deploy() {
        let cli = Cli::try_parse_from(&["fluxx", "deploy", "--provider", "aws", "--region", "us-west-2"]).unwrap();
        match cli.command {
            Commands::Deploy { provider, region, .. } => {
                assert_eq!(provider, "aws");
                assert_eq!(region, Some("us-west-2".to_string()));
            },
            _ => panic!("Expected Deploy command"),
        }
    }
}
