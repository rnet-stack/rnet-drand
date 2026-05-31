use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use rand::{rng, seq::IteratorRandom};
use rnet_p2p::{identity::traits::protocols::INodeFloodsubAPI, node::node::Node};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::common::{MpcMsgType, generate_entropy, unix_epoch};

#[derive(Serialize, Deserialize)]
pub struct SessionPayload {
    pub source: String,
    pub stage: SessionStage,
    pub participants: Option<Vec<String>>,
    pub commit_hash: Option<String>,
    pub secret: Option<String>,
    pub drand: Option<String>,

    pub timestamp: u64,
}

#[derive(Serialize, Deserialize)]
pub enum SessionStage {
    Ack,
    Commit,
    Reveal,
    Reduction,
    Concensus,
}

pub struct SessionState {
    pub stage: SessionStage,
    pub session_id: String,
    pub participants: Vec<String>,
    pub blacklist: Vec<String>,
    pub commit_hashes: HashMap<String, String>,
    pub drand: Option<String>,

    pub is_leader: bool,
    pub commit_deadline: u32,
    pub ack_deadline: u32,
}

pub struct DrandService {
    pub sessions: Arc<Mutex<HashMap<String, SessionState>>>,
    pub host_mpsc_tx: Arc<Node>,
}

impl DrandService {
    pub async fn spawn_session(
        &self,
        session_id: String,
        commit_deadline: u32,
        ack_deadline: u32,
        is_leader: bool,
    ) -> Result<()> {
        debug!("Drand session spawned: {}", session_id);

        // Send out ACK payload, after ack_deadline
        // populate the list of participants
        // send out the Commit payload, before commit_deadline

        let drand_session = SessionState {
            stage: SessionStage::Ack,
            session_id: session_id.clone(),
            participants: Vec::new(),
            blacklist: Vec::new(),
            commit_hashes: HashMap::new(),
            drand: None,
            is_leader,
            commit_deadline: commit_deadline.clone(),
            ack_deadline: ack_deadline.clone(),
        };

        {
            let mut sessions = self.sessions.lock().await;
            sessions.insert(session_id.clone(), drand_session);
        }

        // ---------ACK-SESSION-----------
        {
            debug!("Waiting in for ACK deadline: {}", session_id);
            tokio::time::sleep(Duration::from_secs(ack_deadline as u64)).await;

            if is_leader {
                let mut particpants = self
                    .host_mpsc_tx
                    .floodsub_mesh()
                    .await
                    .unwrap()
                    .get(&session_id)
                    .unwrap()
                    .clone();

                particpants.push(self.host_mpsc_tx.get_local().listen_addr);

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
                    .floodsub_publish(session_id.clone(), payload_bytes)
                    .await
                    .unwrap();

                {
                    let mut session = self.sessions.lock().await;
                    let state = session.get_mut(&session_id).unwrap();
                    state.participants = particpants;
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
            // --------------------------------

            info!("ACK completed, participants for - {session_id}: \n");
            let participants = {
                let session = self.sessions.lock().await;
                let state = session.get(&session_id).unwrap();
                state.participants.clone()
            };

            participants.iter().for_each(|x| {
                println!("    - {x}");
            });
        }

        // Update session-stage
        {
            let mut sessions = self.sessions.lock().await;
            let session = sessions.get_mut(&session_id).unwrap();
            session.stage = SessionStage::Commit;
        }

        // ----------COMMIT-SESSION---------------
        {
            info!("Staring into commit phase: {session_id}");

            let nonce = (1..500).choose(&mut rng()).unwrap();
            tokio::time::sleep(Duration::from_millis(nonce)).await;

            let (_secret, hash) = generate_entropy();
            {
                let mut session = self.sessions.lock().await;
                let state = session.get_mut(&session_id).unwrap();
                state
                    .commit_hashes
                    .insert(self.host_mpsc_tx.get_local().listen_addr, hash.clone());
            }

            let commit_payload = SessionPayload {
                source: self.host_mpsc_tx.get_local().listen_addr,
                stage: SessionStage::Commit,
                participants: None,
                commit_hash: Some(hash),
                secret: None,
                drand: None,
                timestamp: unix_epoch(),
            };

            let fsub_payload = MpcMsgType::Session(commit_payload);
            let payload_bytes = bincode::serialize(&fsub_payload).unwrap();

            self.host_mpsc_tx
                .floodsub_publish(session_id.clone(), payload_bytes)
                .await
                .unwrap();

            debug!("Published hash, waiting for commit deadline...");
            tokio::time::sleep(Duration::from_secs(commit_deadline as u64)).await;
            // ---------------------------------------

            info!("Commit deadline completed, {}", unix_epoch());
        }

        Ok(())
    }

    pub async fn handle_incoming(&self, payload: SessionPayload, session_id: String) -> Result<()> {
        match payload.stage {
            SessionStage::Ack => {
                let participants = payload.participants.unwrap();
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
                warn!("COMMIT: {}, {}", payload.source, unix_epoch());
            }
            _ => {}
        }
        Ok(())
    }
}
