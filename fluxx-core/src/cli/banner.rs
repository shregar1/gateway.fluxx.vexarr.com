//! Beautiful CLI Banner for Fluxx Agent

use std::time::SystemTime;
use uuid::Uuid;

/// Color codes for terminal output
pub mod colors {
    pub const RESET: &str = "\x1b[0m";
    pub const BOLD: &str = "\x1b[1m";
    pub const DIM: &str = "\x1b[2m";
    pub const ITALIC: &str = "\x1b[3m";
    pub const UNDERLINE: &str = "\x1b[4m";
    
    // Colors
    pub const CYAN: &str = "\x1b[36m";
    pub const BLUE: &str = "\x1b[34m";
    pub const GREEN: &str = "\x1b[32m";
    pub const YELLOW: &str = "\x1b[33m";
    pub const MAGENTA: &str = "\x1b[35m";
    pub const RED: &str = "\x1b[31m";
    pub const WHITE: &str = "\x1b[37m";
    
    // Bright colors
    pub const BRIGHT_CYAN: &str = "\x1b[96m";
    pub const BRIGHT_GREEN: &str = "\x1b[92m";
    pub const BRIGHT_YELLOW: &str = "\x1b[93m";
    pub const BRIGHT_MAGENTA: &str = "\x1b[95m";
    pub const BRIGHT_BLUE: &str = "\x1b[94m";
}

use colors::*;

/// ASCII art logos
pub const LOGO_LARGE: &str = r#"
    ███████╗██╗     ██╗   ██╗██╗  ██╗██╗  ██╗
    ██╔════╝██║     ██║   ██║╚██╗██╔╝╚██╗██╔╝
    █████╗  ██║     ██║   ██║ ╚███╔╝  ╚███╔╝ 
    ██╔══╝  ██║     ██║   ██║ ██╔██╗  ██╔██╗ 
    ██║     ███████╗╚██████╔╝██╔╝ ██╗██╔╝ ██╗
    ╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝
"#;

pub const LOGO_COMPACT: &str = r#"
    ╭───────── FLUXX AGENT ─────────╮
    │  ⚡ High-Performance Edge Obs │
    ╰────────────────────────────────╯
"#;

pub const LOGO_MINIMAL: &str = r#"⚡ FLUXX"#;

/// System information struct
#[derive(Debug, Default)]
pub struct SystemInfo {
    pub username: String,
    pub hostname: String,
    pub version: String,
    pub session_id: String,
    pub session_start: Option<SystemTime>,
    pub integrations_count: usize,
    pub events_processed: u64,
    pub memory_usage_mb: Option<u64>,
    pub cpu_usage_percent: Option<f32>,
}

impl SystemInfo {
    pub fn new() -> Self {
        Self {
            username: whoami::username(),
            hostname: whoami::fallible::hostname().unwrap_or_else(|_| "unknown".to_string()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            session_id: format!("flx-{}", Uuid::new_v4().to_string().split('-').next().unwrap_or("xxxx")),
            session_start: Some(SystemTime::now()),
            integrations_count: 45, // Default placeholder
            events_processed: 0,
            memory_usage_mb: None,
            cpu_usage_percent: None,
        }
    }
    
    pub fn with_stats(integrations: usize, events: u64) -> Self {
        let mut info = Self::new();
        info.integrations_count = integrations;
        info.events_processed = events;
        info
    }
    
    /// Get short session ID (first 8 chars)
    pub fn short_session_id(&self) -> &str {
        &self.session_id[..self.session_id.len().min(12)]
    }
}

/// Print the full banner with all information
pub fn print_banner(info: &SystemInfo) {
    println!();
    print_logo_large();
    print_user_info(info);
    print_session_info(info);
    print_stats_bar(info);
    println!();
}

/// Print compact banner for embedded use
pub fn print_compact_banner(info: &SystemInfo) {
    println!();
    print_logo_compact();
    print_compact_info(info);
    println!();
}

/// Print minimal banner
pub fn print_minimal_banner() {
    println!("{BOLD}{BRIGHT_CYAN}{LOGO_MINIMAL}{RESET} {DIM}v{}{RESET}", env!("CARGO_PKG_VERSION"));
}

fn print_logo_large() {
    let lines = LOGO_LARGE.lines();
    for line in lines {
        if !line.trim().is_empty() {
            println!("{BOLD}{BRIGHT_CYAN}{}{RESET}", line);
        }
    }
    println!("{DIM}{:>50}{RESET}", "High-Performance Edge Observability");
    println!();
}

fn print_logo_compact() {
    println!("{BOLD}{CYAN}{}{RESET}", LOGO_COMPACT);
}

fn print_user_info(info: &SystemInfo) {
    let user_text = format!("👤 User: {}", info.username);
    let host_text = format!("🖥️  Host: {}", info.hostname);
    let session_text = format!("🔑 Session: {}", info.short_session_id());
    let version_text = format!("🔖 Version: {}", info.version);
    
    println!("  {BRIGHT_GREEN}{:<28}{RESET} {BRIGHT_YELLOW}{:<35}{RESET}", 
             user_text, host_text);
    println!("  {BRIGHT_MAGENTA}{:<28}{RESET} {BRIGHT_BLUE}{}{RESET}", 
             session_text, version_text);
    println!();
}

fn print_compact_info(info: &SystemInfo) {
    println!("  {DIM}User:{RESET} {CYAN}{}{RESET} {DIM}@{} | {DIM}Session:{RESET} {MAGENTA}{}{RESET} | {DIM}Integrations:{RESET} {GREEN}{}{RESET}",
             info.username, info.hostname, info.short_session_id(), info.integrations_count);
}

fn print_session_info(info: &SystemInfo) {
    if let Some(start) = info.session_start {
        let elapsed = SystemTime::now()
            .duration_since(start)
            .unwrap_or_default();
        let uptime = format_duration(elapsed.as_secs());
        
        println!("  {BRIGHT_MAGENTA}⏱️  Session Uptime:{RESET} {}", uptime);
    }
    
    // Display resource usage if available
    if let Some(mem) = info.memory_usage_mb {
        print!("  {DIM}Memory:{RESET} {YELLOW}{} MB{RESET}", mem);
        if let Some(cpu) = info.cpu_usage_percent {
            println!("  {DIM}CPU:{RESET} {YELLOW}{:.1}%{RESET}", cpu);
        } else {
            println!();
        }
    }
    println!();
}

fn print_stats_bar(info: &SystemInfo) {
    let width = 60;
    
    println!("  {DIM}{}{RESET}", "─".repeat(width));
    
    // Stats row
    let integrations = format!("{BRIGHT_GREEN}●{RESET} Integrations: {BOLD}{}{RESET}", info.integrations_count);
    let events = format!("{BRIGHT_CYAN}⚡{RESET} Events: {BOLD}{}{RESET}", format_number(info.events_processed));
    let status = format!("{BRIGHT_GREEN}✓{RESET} Status: {BOLD}Active{RESET}");
    
    println!("  {:<20} {:<20} {}", integrations, events, status);
    
    // Progress bar for visual effect
    print!("  {DIM}[{RESET}");
    let filled = (info.events_processed % 20) as usize + 3;
    for i in 0..20 {
        if i < filled {
            print!("{BRIGHT_CYAN}█{RESET}");
        } else {
            print!("{DIM}░{RESET}");
        }
    }
    println!("{DIM}]{RESET}");
    
    println!("  {DIM}{}{RESET}", "─".repeat(width));
}

/// Format large numbers with commas
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let mut count = 0;
    
    for c in s.chars().rev() {
        if count == 3 {
            result.push(',');
            count = 0;
        }
        result.push(c);
        count += 1;
    }
    
    result.chars().rev().collect()
}

/// Format duration in human-readable form
fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else if seconds < 86400 {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    } else {
        format!("{}d {}h", seconds / 86400, (seconds % 86400) / 3600)
    }
}

/// Print a simple welcome message (for start command)
pub fn print_welcome() {
    println!();
    println!("  {BOLD}{BRIGHT_CYAN}Welcome to Fluxx Agent{RESET}");
    println!("  {DIM}High-performance observability for the edge{RESET}");
    println!();
}

/// Print shutdown banner
pub fn print_shutdown() {
    println!();
    println!("  {BRIGHT_YELLOW}⚡ Fluxx Agent shutting down...{RESET}");
    println!("  {DIM}Goodbye! 👋{RESET}");
    println!();
}

/// Print error banner
pub fn print_error(message: &str) {
    println!();
    println!("  {BOLD}{RED}✗ Error{RESET}");
    println!("  {RED}{}{RESET}", message);
    println!();
}

/// Print success banner
pub fn print_success(message: &str) {
    println!("  {BOLD}{BRIGHT_GREEN}✓ {}{RESET}", message);
}

/// Print info line
pub fn print_info(label: &str, value: &str) {
    println!("  {DIM}{}:{RESET} {}{}", label, BOLD, value);
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_format_number() {
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1000000), "1,000,000");
        assert_eq!(format_number(42), "42");
    }
    
    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3661), "1h 1m");
    }
    
    #[test]
    fn test_system_info_new() {
        let info = SystemInfo::new();
        assert!(!info.username.is_empty());
        assert!(!info.hostname.is_empty());
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert!(info.session_id.starts_with("flx-"));
        assert!(!info.session_id.is_empty());
    }
    
    #[test]
    fn test_short_session_id() {
        let info = SystemInfo::new();
        let short = info.short_session_id();
        assert!(short.len() <= 12);
        assert!(short.starts_with("flx-"));
    }
}
