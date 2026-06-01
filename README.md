# rnet-drand

A distributed random number generator leveraging multi-party computation (MPC), built on the [rnet-p2p](https://github.com/rnet-stack/rnet-p2p) stack.

This allows a group of peers to collectively generate verifiable randomness without relying on a trusted central authority.

---

### DRAND session with 1 `BOOTSTRAP` and 3 `PARTICIPANTS` 
This latest iteration successfully demonstrates a drand session including 3 partipant nodes contributing entrophy, and generating an unbiased MPC random-number.

https://github.com/user-attachments/assets/6aaa413e-5779-4cd6-864b-5b713fbdfa8b

## Why?

In distributed systems, randomness is surprisingly hard. If a single node generates the random value, that node can:

- Bias outcomes in its favour
- Reroll unfavourable values
- Manipulate leader selection
- Influence lotteries or matchmaking
  With `rnet-dice`, multiple peers collaborate to generate randomness using a **commit-reveal protocol**. As long as at least **one peer remains honest**, the final beacon remains unpredictable and unmanipulable.

---

## Architecture Overview
 
The system has two node roles:
 
**Bootstrap Node** — a well-known node that listens on a fixed address (`127.0.0.1:8888`). It maintains and periodically broadcasts a **bootmesh**: a map of which peers are subscribed to which topics. This gives joining peers a way to discover who is participating in a session without relying on a central registry.
 
**General Node** — a regular participant. On startup it connects to the bootstrap node, subscribes to the `mpc-common` coordination topic, and then participates in drand sessions.
 
All communication happens over **Floodsub** — a publish/subscribe gossip protocol. Every message is serialised with `bincode` and typed via `MpcMsgType`:
 
| Variant | Purpose |
|---|---|
| `General(msg)` | Plain text debug/chat messages |
| `Advertize((id, commit_deadline, ack_deadline))` | Announce a new drand session |
| `Session(payload)` | Carry a protocol-stage payload (Ack / Commit / Reveal / Reduction) |
| `Bootmesh(mesh)` | Bootstrap node broadcasting the peer map |
 
---


## The Pipeline
 
```
adv  →  con  →  commit  →  reveal  →  reduce
```
 
### 1. `adv <topic>` — Advertise
 
The **leader** broadcasts a `MpcMsgType::Advertize` to `mpc-common` with the session topic and deadlines, then subscribes to the `session-id`. Other nodes see the advertisement and decide whether to join.
 
### 2. `con <topic>` — Join
 
A **participant** subscribes to the session topic, then looks up the bootmesh to find existing members. It connects directly to the leader and also to a randomly chosen peer — an intentional anti-Eclipse-Attack measure to ensure at least one independent route into the mesh.
 
### 3. `commit <topic>` — Commit
 
The leader first publishes a `SessionStage::Ack` listing all known participants, waits for it to propagate, then generates a `(secret, hash)` pair and broadcasts only the **hash**. Each participant does the same on receipt — generating its own pair and publishing only its hash. No secrets are exposed yet; everyone is locked into their contribution. A random jitter (`1..500 ms`) is applied before each publish to smooth out gossip collisions.
 
### 4. `reveal <topic>` — Reveal
 
The leader publishes its secret, which triggers each participant to publish theirs. Every incoming secret is verified: `SHA-256(secret) == committed hash`. Peers that pass are accepted; peers that fail are **blacklisted** and excluded. Because the hash was broadcast before the secret, no one can reroll after seeing others' values.
 
### 5. `reduce <topic>` — Reduction
 
All accepted hashes are folded into a single value via `xor_sha_commits`. The leader broadcasts the result; every participant independently computes the same reduction and cross-checks it. If they agree, the **drand beacon** is confirmed.
 
---

## CLI Reference
 
| Command | Description |
|---|---|
| `help` | Print all commands |
| `local` | Show local peer address |
| `connect <maddr>` | Connect to a peer |
| `ping <maddr> <count>` | Ping a peer |
| `peers` | List connected peers |
| `fsub <maddr>` | Open a Floodsub stream to a peer |
| `join <topic>` | Subscribe to a topic |
| `leave <topic>` | Unsubscribe from a topic |
| `publish <topic> <msg>` | Publish a raw message to a topic |
| `topics` | List subscribed topics |
| `fpeers` | List Floodsub peers |
| `bootmesh` | Print the bootstrap peer map |
| `mesh` | Print the local Floodsub mesh |
| `adv <topic>` | Advertise a new drand session (leader) |
| `con <topic>` | Join an existing drand session |
| `commit <topic>` | Trigger the commit phase (leader) |
| `reveal <topic>` | Manually trigger the reveal phase |
| `reduce <topic>` | Manually trigger the reduction phase |
 
---

## Security Properties
 
**Unpredictability** — No single node knows the final beacon before the reveal phase, because every participant's secret is hidden behind a hash commitment.
 
**Unbiasability** — A dishonest leader or participant cannot skew the output. They committed to their hash before seeing anyone else's secret. Any attempt to substitute a different secret is caught by the hash check and the peer is blacklisted.
 
**Liveness** — A blacklisted or absent peer is simply excluded from the reduction. The protocol completes as long as at least one honest peer contributes.
 
**Eclipse-Attack Resistance** — Participants connect to both the leader and a randomly selected peer, making it harder for any single adversarial node to control a participant's entire view of the network.
 
---
