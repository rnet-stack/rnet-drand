use anyhow::Result;
use rand::{RngExt, distr::Alphanumeric, rng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::drand::SessionPayload;

pub type SessionId = String;
pub type CommitDeadline = u64;
pub type AckDeadline = u64;

pub fn set_bootstrap_node(addr: &str) -> Result<()> {
    let env_path = ".env";
    let content = fs::read_to_string(env_path).unwrap_or_default();
    let mut found = false;

    let mut lines: Vec<String> = content
        .lines()
        .map(|line| {
            if line.starts_with("BOOTSTRAP_NODE=") {
                found = true;
                format!("BOOTSTRAP_NODE={}", addr)
            } else {
                line.to_string()
            }
        })
        .collect();

    if !found {
        lines.push(format!("BOOTSTRAP_NODE={}", addr));
    }

    fs::write(env_path, lines.join("\n"))?;
    Ok(())
}

pub fn unix_epoch() -> u64 {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap();

    duration.as_secs()
}

pub fn generate_entropy() -> (String, String) {
    let secret: String = rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect();

    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    let hash = hex::encode(hasher.finalize());

    (secret, hash)
}

#[derive(Serialize, Deserialize)]
pub enum MpcMsgType {
    General(String),
    Session(SessionPayload),
    Advertize((SessionId, CommitDeadline, AckDeadline)),
    Bootmesh(HashMap<String, Vec<String>>),
}
