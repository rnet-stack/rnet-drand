use std::io::{self, Write};

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use crate::{cli::cli_loop, node::MPCNode};

pub mod cli;
pub mod common;
pub mod drand;
pub mod node;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("trace"))
        .without_time()
        .with_target(false)
        .compact()
        .init();

    dotenvy::dotenv().ok();

    print!("Start as bootstrap node (y/n): ");
    io::stdout().flush().unwrap();

    let mut mode = String::new();
    io::stdin().read_line(&mut mode).unwrap();

    if mode.trim().to_lowercase() != "" {
        mode = "general".to_string();
    } else {
        mode = "bootstrap".to_string();
    }

    let mcp_node = MPCNode::new(mode.as_str()).await;
    cli_loop(mcp_node.clone()).await.unwrap();

    Ok(())
}
