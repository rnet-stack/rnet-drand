use std::{collections::HashMap, env, sync::Arc, time::Duration};

use anyhow::Result;
use rnet_p2p::{
    identity::{
        events::{FloodsubMsgType, GlobalEvent},
        multiaddr::Multiaddr,
        traits::{core::INode, protocols::INodeFloodsubAPI},
    },
    node::{inner::NodeInner, node::Node, protocol::InnerProtocolOpt},
    protocols::FLOODSUB,
};
use tokio::sync::{Mutex, mpsc::Receiver};
use tracing::{debug, info};

use crate::{common::MpcMsgType, drand::DrandService};

pub struct MPCNode {
    pub host_mpsc_tx: Arc<Node>,
    pub mode: String,
    pub listen_addr: Multiaddr,
    pub drand_service: Arc<DrandService>,

    pub bootmesh: Arc<Mutex<HashMap<String, Vec<String>>>>,
}

impl MPCNode {
    pub async fn new(mode: &str) -> Arc<MPCNode> {
        debug!("Running as {} node", mode.to_uppercase());

        let (host_mpsc_tx, global_rx, listen_addr) = match mode.as_ref() {
            "bootstrap" => {
                let mut listen_addr = Multiaddr::new("ip4/127.0.0.1/tcp/8888").unwrap();
                let key_hex = env::var("BOOTSTRAP_PVT_KEY").unwrap();

                let (host_mpsc_tx, global_rx) = NodeInner::new(
                    &mut listen_addr,
                    vec![InnerProtocolOpt::Floodsub, InnerProtocolOpt::Ping],
                    Some(key_hex),
                )
                .await
                .unwrap();

                (host_mpsc_tx, global_rx, listen_addr)
            }
            _ => {
                let mut listen_addr = Multiaddr::new("ip4/127.0.0.1/tcp/0").unwrap();
                let (host_mpsc_tx, global_rx) = NodeInner::new(
                    &mut listen_addr,
                    vec![InnerProtocolOpt::Floodsub, InnerProtocolOpt::Ping],
                    None,
                )
                .await
                .unwrap();

                (host_mpsc_tx, global_rx, listen_addr)
            }
        };

        let mpc_node = Arc::new(MPCNode {
            host_mpsc_tx: host_mpsc_tx.clone(),
            mode: mode.to_string(),
            listen_addr,
            drand_service: Arc::new(DrandService {
                sessions: Arc::new(Mutex::new(HashMap::new())),
                host_mpsc_tx: host_mpsc_tx.clone(),
            }),

            bootmesh: Arc::new(Mutex::new(HashMap::new())),
        });
        let handler_mcp = mpc_node.clone();

        tokio::spawn(async move {
            handler_mcp.p2p_handler(global_rx).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(2000)).await;

        let mesh_mpc_node = mpc_node.clone();
        mpc_node.initiate(mesh_mpc_node).await.unwrap();

        mpc_node
    }

    pub async fn initiate(&self, mesh_mpc_node: Arc<MPCNode>) -> Result<()> {
        self.host_mpsc_tx
            .floodsub_subscribe("mpc-common".to_string())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2000)).await;

        match self.mode.as_ref() {
            "bootstrap" => {
                tokio::spawn(async move {
                    mesh_mpc_node.periodic_mesh_update().await.unwrap();
                });
            }
            "general" => {
                // CONNECT TO BOOTSTRAP NODE
                let bootstrap_node = Multiaddr::new(&env::var("BOOTSTRAP_NODE").unwrap()).unwrap();
                info!("BOOTSTRAP node found, CONNECTING...\n");
                self.host_mpsc_tx
                    .new_stream(&bootstrap_node.to_string(), vec![FLOODSUB.to_string()])
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(2000)).await;
            }
            _ => {}
        }

        Ok(())
    }

    pub async fn p2p_handler(&self, mut global_event_rx: Receiver<Vec<u8>>) -> Result<()> {
        loop {
            let notification = global_event_rx.recv().await.unwrap();
            let decoded = bincode::deserialize::<GlobalEvent>(&notification).unwrap();
            match decoded {
                GlobalEvent::Floodsub(event) => match event.msg_type {
                    FloodsubMsgType::Publish => {
                        let topic = event.topic.clone();
                        let source = event.source.unwrap();
                        let decoded_msg =
                            bincode::deserialize::<MpcMsgType>(&event.msg.unwrap()).unwrap();

                        match decoded_msg {
                            MpcMsgType::General(msg) => {
                                debug!("FloodsubEvent: {topic} - {source}: {msg}")
                            }

                            MpcMsgType::Advertize((id, commit, ack)) => {
                                if self.mode == "bootstrap" {
                                    continue;
                                }

                                info!(
                                    "New Drand session - session_id: {id}; ack_deadline: {ack}; commit_deadline: {commit}"
                                );
                            }

                            MpcMsgType::Session(payload) => {
                                self.drand_service
                                    .handle_incoming(payload, event.topic)
                                    .await
                                    .unwrap();
                            }
                            MpcMsgType::Bootmesh(mesh) => {
                                let mut bootmesh = self.bootmesh.lock().await;
                                *bootmesh = mesh;

                                debug!("BOOTMESH updated");
                            }
                        }
                    }
                    FloodsubMsgType::Subscribe => {
                        // debug!("FloodsubEvent: SUBSCRIBED - {}", event.topic);
                    }
                    FloodsubMsgType::Unsubscribe => {
                        debug!("FloodsubEvent: UNSUBSCRIBED - {}", event.topic);
                    }
                },
                GlobalEvent::Ping(event) => debug!("{:?}", event),
            }
        }
    }

    pub async fn periodic_mesh_update(&self) -> Result<()> {
        loop {
            let bootmesh = {
                let bootmesh = self.bootmesh.lock().await;

                bootmesh.clone()
            };

            let latest_mesh = self
                .host_mpsc_tx
                .floodsub_mesh()
                .await
                .unwrap_or(HashMap::new());

            if bootmesh == latest_mesh {
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }

            {
                // Update the BOOTMESH
                let mut bootmesh = self.bootmesh.lock().await;
                *bootmesh = latest_mesh.clone();
            }

            self.broadcast_bootmesh(latest_mesh).await.unwrap();
        }
    }

    pub async fn broadcast_bootmesh(&self, mesh: HashMap<String, Vec<String>>) -> Result<()> {
        let fsub_msg = MpcMsgType::Bootmesh(mesh);
        let payload = bincode::serialize(&fsub_msg).unwrap();

        // Wait 2 seconds for the new node to settle down
        tokio::time::sleep(Duration::from_secs(2)).await;

        debug!("BOOTMESH updated");

        self.host_mpsc_tx
            .floodsub_publish("mpc-common".to_string(), payload)
            .await
            .unwrap();

        Ok(())
    }
}
