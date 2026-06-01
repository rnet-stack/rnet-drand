use anyhow::Result;
use rand::rng;
use rand::seq::IndexedRandom;
use rnet_p2p::identity::multiaddr::Multiaddr;
use rnet_p2p::identity::traits::{
    core::INode,
    protocols::{INodeFloodsubAPI, INodePingAPI},
};
use std::collections::HashMap;
use std::{io::Write, sync::Arc, time::Duration};
use tokio::io::{self, AsyncBufReadExt};
use tracing::debug;

use crate::common::{MpcMsgType, unix_epoch};
use crate::drand::{SessionPayload, SessionStage, SessionState};
use crate::node::MPCNode;

const CLI_DELAY: Duration = Duration::from_nanos(1000);
const FLOODSUB: &str = "rnet/floodsub/0.0.1";
const COMMANDS: &[&str] = &[
    "help                       => print all the commands",
    "local                      => get local peer-info",
    "connect <maddr>            => connect with a new peer",
    "ping <maddr> <count>       => exchange ping with a peer",
    "peers                      => list the connected peers",
    "\n",
    "fsub <maddr>               => open a new floodsub stream with the peer",
    "join <topic>               => subscribe to a new-topic",
    "leave <topic>              => unsubscribe to a new-topic",
    "publish <topic> <msg>      => publish a msg to a topic",
    "topics                     => list the subscribed topics",
    "fpeers                     => list the connected Floodsub peers",
    "bootmesh                   => map of topics -> peer (BOOTSTRAP)",
    "mesh                       => map of topics -> peer",
    "\n",
    "adv <topic>                => advertize a drand session",
    "con <topic>                => participate in a drand session",
    "commit <topic>             => commit the random hash",
    "reveal <topic>             => reveal the secrets",
    "reduce <topic>             => reduce the hashes, and general the mpc drand",
];

fn print_commands() {
    for cmd in COMMANDS {
        println!("      {}", cmd);
    }
}

async fn handle_cmd(line: &str, mpc_node: &Arc<MPCNode>) -> Result<()> {
    let mut parts = line.split_whitespace();
    let cmd = parts.next().unwrap();

    match cmd {
        "help" => {
            print_commands();
        }

        "local" => {
            let peer_info = mpc_node.host_mpsc_tx.get_local();
            println!("{}", peer_info.listen_addr);
        }

        "connect" => {
            let maddr = Multiaddr::new(parts.next().unwrap()).unwrap();
            mpc_node.host_mpsc_tx.connect(&maddr).await?;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        "ping" => {
            let maddr = parts.next().unwrap();
            let count: u32 = parts.next().unwrap_or("0").parse().unwrap_or(0);
            mpc_node
                .host_mpsc_tx
                .ping(Some(count), maddr)
                .await
                .unwrap();
        }

        "fsub" => {
            let maddr = parts.next().unwrap();
            mpc_node
                .host_mpsc_tx
                .new_stream(maddr, vec![FLOODSUB.to_string()])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        "join" => {
            let topic = parts.next().unwrap().to_string();
            mpc_node
                .host_mpsc_tx
                .floodsub_subscribe(topic)
                .await
                .unwrap();
        }

        "leave" => {
            let topic = parts.next().unwrap().to_string();
            mpc_node
                .host_mpsc_tx
                .floodsub_unsubscribe(vec![topic])
                .await
                .unwrap();
        }

        "publish" => {
            let topic = parts.next().unwrap().to_string();
            let msg = parts.collect::<Vec<_>>().join(" ");

            // Wrap this into MpcMsgType::General
            let mpc_general = MpcMsgType::General(msg);
            let payload = bincode::serialize(&mpc_general).unwrap();

            mpc_node
                .host_mpsc_tx
                .floodsub_publish(topic, payload)
                .await
                .unwrap();
        }

        "topics" => {
            let topics = mpc_node
                .host_mpsc_tx
                .floodsub_topics()
                .await
                .unwrap_or(vec![]);
            println!("{:?}", topics);
        }

        "fpeers" => {
            let fpeers = mpc_node
                .host_mpsc_tx
                .floodsub_peers()
                .await
                .unwrap_or(vec![]);
            fpeers.iter().for_each(|x| {
                println!("{}", x);
            });
        }

        "bootmesh" => {
            let bootmesh = mpc_node.bootmesh.lock().await.clone();

            bootmesh.iter().for_each(|(topic, peers)| {
                println!("- {}", topic);
                peers.iter().for_each(|peer| println!("  - {}", peer));
            });
        }

        "peers" => {
            let gpeers = mpc_node.host_mpsc_tx.get_peers().await;
            gpeers
                .iter()
                .for_each(|peer| println!("{}", peer.to_string()));
        }

        "mesh" => {
            let mesh = mpc_node
                .host_mpsc_tx
                .floodsub_mesh()
                .await
                .unwrap_or(HashMap::new());

            mesh.iter().for_each(|(topic, peers)| {
                println!("- {}", topic);
                peers.iter().for_each(|peer| println!("  - {}", peer));
            });
        }

        "adv" => {
            let topic = parts.next().unwrap().to_string();
            let fsub_msg =
                bincode::serialize(&MpcMsgType::Advertize((topic.clone(), 15, 15))).unwrap();

            mpc_node
                .host_mpsc_tx
                .floodsub_publish("mpc-common".to_string(), fsub_msg)
                .await
                .unwrap();

            mpc_node
                .host_mpsc_tx
                .floodsub_subscribe(topic.clone())
                .await
                .unwrap();

            let drand_session = SessionState {
                stage: SessionStage::Ack,
                session_id: topic.clone(),
                participants: Vec::new(),
                blacklist: Vec::new(),
                commit_hashes: HashMap::new(),
                drand: None,

                local_rand: None,
                leader: mpc_node.host_mpsc_tx.local_peer_info.listen_addr.clone(),
                is_leader: true,
            };

            {
                let mut sessions = mpc_node.drand_service.sessions.lock().await;
                sessions.insert(topic.clone(), drand_session);
            }

            debug!("ADVERTIZE done, waiting on the partipants...");
        }

        "con" => {
            let topic = parts.next().unwrap().to_string();
            debug!("Participating in drand session: {}", topic);
            mpc_node
                .host_mpsc_tx
                .floodsub_subscribe(topic.clone())
                .await
                .unwrap();

            let peers = {
                let bootmesh = mpc_node.bootmesh.lock().await;
                bootmesh.get(&topic).unwrap_or(&vec![]).clone()
            };

            let leader = peers[0].clone();
            debug!("Connecting to leader: {}", leader);
            mpc_node
                .host_mpsc_tx
                .new_stream(leader.as_ref(), vec![FLOODSUB.to_string()])
                .await
                .unwrap();

            let random_peer = peers.choose(&mut rng()).expect("No peer to connect to");
            debug!(
                "Connecting to a random peer, to prevent Eclipse Attack: {}",
                random_peer
            );

            mpc_node
                .host_mpsc_tx
                .new_stream(random_peer, vec![FLOODSUB.to_string()])
                .await
                .unwrap();

            let drand_session = SessionState {
                stage: SessionStage::Ack,
                session_id: topic.clone(),
                participants: Vec::new(),
                blacklist: Vec::new(),
                commit_hashes: HashMap::new(),
                drand: None,

                local_rand: None,
                leader,
                is_leader: false,
            };

            {
                let mut sessions = mpc_node.drand_service.sessions.lock().await;
                sessions.insert(topic.clone(), drand_session);
            }

            debug!("ACK completed, waiting for leader to start COMMIT...");
        }

        "commit" => {
            let topic = parts.next().unwrap().to_string();

            mpc_node
                .drand_service
                .ack_participants(&topic)
                .await
                .unwrap();

            debug!("Initiating the commit phase: {topic}");
            tokio::time::sleep(Duration::from_secs(2)).await;

            mpc_node
                .drand_service
                .handle_commit(None, &topic)
                .await
                .unwrap();
        }

        "reveal" => {
            let topic = parts.next().unwrap().to_string();

            let (secret, _) = {
                let mut sessions = mpc_node.drand_service.sessions.lock().await;
                let session = sessions.get_mut(&topic).unwrap();
                session.stage = SessionStage::Reduction;

                session.local_rand.clone().unwrap()
            };

            let reveal_payload = SessionPayload {
                source: mpc_node.host_mpsc_tx.local_peer_info.listen_addr.clone(),
                stage: SessionStage::Reveal,
                participants: None,
                commit_hash: None,
                secret: Some(secret),
                drand: None,
                timestamp: unix_epoch(),
            };

            let fsub_payload = MpcMsgType::Session(reveal_payload);
            let payload_bytes = bincode::serialize(&fsub_payload).unwrap();

            mpc_node
                .host_mpsc_tx
                .floodsub_publish(topic, payload_bytes)
                .await
                .unwrap();
        }

        "reduce" => {
            let topic = parts.next().unwrap().to_string();

            mpc_node
                .drand_service
                .handle_reduction(&topic, None)
                .await
                .unwrap();
        }

        _ => println!("Unknown command"),
    }
    Ok(())
}

pub async fn cli_loop(mpc_node: Arc<MPCNode>) -> Result<()> {
    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    println!("\n MPC_NODE CLI ready. Commands:");
    print_commands();

    loop {
        print!("\nCommand => ");
        std::io::stdout().flush().unwrap();

        let line = match lines.next_line().await? {
            Some(line) => line,
            None => break,
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        handle_cmd(line, &mpc_node).await?;
        tokio::time::sleep(CLI_DELAY).await;
    }

    Ok(())
}
