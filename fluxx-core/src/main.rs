mod install;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    fluxx_agent::cli::run().await
}
