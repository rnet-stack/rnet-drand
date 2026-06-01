use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use rand::{rng, seq::IteratorRandom};
use rnet_p2p::{identity::traits::protocols::INodeFloodsubAPI, node::node::Node};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::common::{MpcMsgType, generate_entropy, unix_epoch, xor_sha_commits};

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionPayload {
    pub source: String,
    pub stage: SessionStage,
    pub participants: Option<Vec<String>>,
    pub commit_hash: Option<String>,
    pub secret: Option<String>,
    pub drand: Option<String>,

    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SessionStage {
    Ack,
    Commit,
    Reveal,
    Reduction,
    Concensus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionState {
    pub stage: SessionStage,
    pub session_id: String,
    pub participants: Vec<String>,
    pub blacklist: Vec<String>,
    pub commit_hashes: HashMap<String, String>,
    pub drand: Option<String>,

    pub local_rand: Option<(String, String)>,
    pub leader: String,
    pub is_leader: bool,
}

pub struct DrandService {
    pub sessions: Arc<Mutex<HashMap<String, SessionState>>>,
    pub host_mpsc_tx: Arc<Node>,
}

impl DrandService {
    pub async fn ack_participants(&self, session_id: &str) -> Result<()> {
        let mut particpants = self
            .host_mpsc_tx
            .floodsub_mesh()
            .await
            .unwrap()
            .get(session_id)
            .unwrap()
            .clone();

        particpants.push(self.host_mpsc_tx.get_local().listen_addr);

        info!("Participants for - {session_id}: \n");
        particpants.iter().for_each(|x| {
            println!("    - {x}");
        });

        let ack_payload = SessionPayload {
            source: self.host_mpsc_tx.get_local().listen_addr,
            stage: SessionStage::Ack,
            participants: Some(particpants.clone()),
            commit_hash: None,
            secret: None,
            drand: None,
            timestamp: unix_epoch(),
        };

        let fsub_payload = MpcMsgType::Session(ack_payload);
        let payload_bytes = bincode::serialize(&fsub_payload).unwrap();

        self.host_mpsc_tx
            .floodsub_publish(session_id.to_string(), payload_bytes)
            .await
            .unwrap();

        {
            let mut session = self.sessions.lock().await;
            let state = session.get_mut(session_id).unwrap();
            state.participants = particpants;
        }

        Ok(())
    }

    pub async fn handle_commit(
        &self,
        payload_opt: Option<SessionPayload>,
        session_id: &str,
    ) -> Result<()> {
        let nonce = (1..500).choose(&mut rng()).unwrap();
        tokio::time::sleep(Duration::from_millis(nonce)).await;

        let leader = {
            let mut session = self.sessions.lock().await;
            let state = session.get_mut(session_id).unwrap();

            state.leader.clone()
        };

        let mut commit_payload = SessionPayload {
            source: self.host_mpsc_tx.get_local().listen_addr,
            stage: SessionStage::Commit,
            participants: None,
            commit_hash: None,
            secret: None,
            drand: None,
            timestamp: unix_epoch(),
        };

        match payload_opt.is_none() {
            false => {
                let payload = payload_opt.unwrap();

                warn!("COMMIT: {}, {}", payload.source, unix_epoch());
                let remote_hash = payload.commit_hash.unwrap();
                let source = payload.source;

                match source == leader {
                    true => {
                        let (local_secret, local_hash) = generate_entropy();
                        {
                            let mut sessions = self.sessions.lock().await;
                            let session = sessions.get_mut(session_id).unwrap();
                            session.stage = SessionStage::Commit;
                            session.commit_hashes.insert(
                                self.host_mpsc_tx.get_local().listen_addr,
                                local_hash.clone(),
                            );

                            session.local_rand = Some((local_secret, local_hash.clone()));
                            session.commit_hashes.insert(source, remote_hash.clone());
                        }

                        commit_payload.commit_hash = Some(local_hash);

                        let fsub_payload = MpcMsgType::Session(commit_payload);
                        let payload_bytes = bincode::serialize(&fsub_payload).unwrap();

                        self.host_mpsc_tx
                            .floodsub_publish(session_id.to_string(), payload_bytes)
                            .await
                            .unwrap();

                        debug!("Entrophy COMMITED, waiting for leader to REVEAL...");
                    }
                    false => {
                        let mut sessions = self.sessions.lock().await;
                        let session = sessions.get_mut(session_id).unwrap();

                        session.commit_hashes.insert(source, remote_hash);
                    }
                }
            }
            true => {
                let (secret, hash) = generate_entropy();

                {
                    let mut sessions = self.sessions.lock().await;
                    let session = sessions.get_mut(session_id).unwrap();
                    session.stage = SessionStage::Commit;
                    session
                        .commit_hashes
                        .insert(self.host_mpsc_tx.get_local().listen_addr, hash.clone());

                    session.local_rand = Some((secret, hash.clone()));
                }

                commit_payload.commit_hash = Some(hash);

                let fsub_payload = MpcMsgType::Session(commit_payload);
                let payload_bytes = bincode::serialize(&fsub_payload).unwrap();

                self.host_mpsc_tx
                    .floodsub_publish(session_id.to_string(), payload_bytes)
                    .await
                    .unwrap();

                debug!("Entrophy COMMITED, waiting for participants...");
            }
        };

        Ok(())
    }

    pub async fn handle_reveal(&self, payload: SessionPayload, session_id: &str) -> Result<()> {
        let nonce = (1..500).choose(&mut rng()).unwrap();
        tokio::time::sleep(Duration::from_millis(nonce)).await;

        let source = payload.source;

        {
            let mut sessions = self.sessions.lock().await;
            let session = sessions.get_mut(session_id).unwrap();
            let (secret, _) = session.local_rand.clone().unwrap();

            match session.leader == source {
                true => {
                    let reveal_payload = SessionPayload {
                        source: self.host_mpsc_tx.local_peer_info.listen_addr.clone(),
                        stage: SessionStage::Reveal,
                        participants: None,
                        commit_hash: None,
                        secret: Some(secret),
                        drand: None,
                        timestamp: unix_epoch(),
                    };

                    let fsub_payload = MpcMsgType::Session(reveal_payload);
                    let payload_bytes = bincode::serialize(&fsub_payload).unwrap();

                    self.host_mpsc_tx
                        .floodsub_publish(session_id.to_string(), payload_bytes)
                        .await
                        .unwrap();

                    session.stage = SessionStage::Reveal;
                }
                _ => {}
            }

            let remote_secret = payload.secret.unwrap();
            let remote_hash = session.commit_hashes.get(&source).unwrap().clone();

            // Compare hashes
            let mut hasher = Sha256::new();
            hasher.update(remote_secret.as_bytes());
            let remote_local_hash = hex::encode(hasher.finalize());

            if remote_hash != remote_local_hash {
                warn!("Dishonest peer, blacklisted: {source}");

                session.blacklist.push(source);
                return Ok(());
            }

            info!("Honest peer, proceeded: {source}");
        }

        Ok(())
    }

    pub async fn handle_reduction(
        &self,
        session_id: &str,
        payload_opt: Option<SessionPayload>,
    ) -> Result<()> {
        debug!("Initiation REDUCTION phase: {}", session_id);

        let nonce = (1..500).choose(&mut rng()).unwrap();
        tokio::time::sleep(Duration::from_millis(nonce)).await;

        let hashes: Vec<String> = {
            let mut sessions = self.sessions.lock().await;
            let stage = sessions.get_mut(session_id).unwrap();
            stage.stage = SessionStage::Reduction;

            stage.commit_hashes.values().cloned().collect()
        };

        let drand = xor_sha_commits(hashes);

        match payload_opt.is_none() {
            false => {
                let payload = payload_opt.unwrap();
                let remote_drand = payload.drand.unwrap();

                if drand == remote_drand {
                    info!(
                        "DRAND session completed: {},\nGENERATED: {}",
                        session_id, drand
                    );
                } else {
                    warn!(
                        "DRAND session failed: {}, \n REMOTE: {}, \n LOCAL: {}",
                        session_id, remote_drand, drand
                    );
                }
            }
            true => {
                let reduction_payload = SessionPayload {
                    source: self.host_mpsc_tx.local_peer_info.listen_addr.clone(),
                    stage: SessionStage::Reduction,
                    participants: None,
                    commit_hash: None,
                    secret: None,
                    drand: Some(drand.clone()),
                    timestamp: unix_epoch(),
                };

                let fsub_payload = MpcMsgType::Session(reduction_payload);
                let payload_bytes = bincode::serialize(&fsub_payload).unwrap();

                self.host_mpsc_tx
                    .floodsub_publish(session_id.to_string(), payload_bytes)
                    .await
                    .unwrap();

                info!(
                    "DRAND session completed: {}, \nGENERATED: {}",
                    session_id, drand
                );
            }
        };

        Ok(())
    }

    pub async fn handle_incoming(&self, payload: SessionPayload, session_id: String) -> Result<()> {
        match payload.stage {
            SessionStage::Ack => {
                let participants = payload.participants.unwrap();
                debug!("Received ACK from leader");

                info!("Participants for - {session_id}: \n");
                participants.iter().for_each(|x| {
                    println!("    - {x}");
                });

                {
                    let mut sessions = self.sessions.lock().await;
                    let session = sessions.get_mut(&session_id).unwrap();

                    match session.stage {
                        SessionStage::Ack => {
                            session.participants = participants;
                        }
                        _ => return Ok(()),
                    }
                }
            }

            SessionStage::Commit => {
                self.handle_commit(Some(payload), &session_id)
                    .await
                    .unwrap();
            }

            SessionStage::Reveal => self.handle_reveal(payload, &session_id).await.unwrap(),
            SessionStage::Reduction => self
                .handle_reduction(&session_id, Some(payload))
                .await
                .unwrap(),

            _ => {}
        }
        Ok(())
    }
}
