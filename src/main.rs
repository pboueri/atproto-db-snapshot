use anyhow::Result;
use at_snapshot::cli;

#[tokio::main]
async fn main() -> Result<()> {
    cli::run().await
}
