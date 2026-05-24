# Rnet-dice

A distributed random number generator leveraging multi-party computation build using the [rnet-p2p](https://github.com/rnet-stack/rnet-p2p) stack.

This allows a group of peers to collectively generate verifiable randomness without relying on a trusted central authority.

## Why ? 

In distributed systems, randomness is suprisingly hard.  If a single node generates the random value, that node can:
- bias outcomes
- reroll unfavourable values
- manipulate leader selection
- influence lotteries or matchmaking

Via this project, multiple peers collaborate to generate randomess using a commit-reveal protocol. As long a atleast 1 peer remains honest, the final beacon remains unpredictable.

### Protocol Overview

Each round happens in 3 phases:
1. **Commit Phase**: Each peer - 
    - generates a random secret
    - hashes the secret
    - broadcasts only the hash

    This locks peers into their contribution without revealing it. All the contributions after a pre-defined timestamp is ignored and rejected from the session.

2. **Reveal Phase**: 
    - peers reveal their original secrets
    - all participants verify, show `hash(secret) == prior_commit`

3. **Reduction Phase**: All the valid peer combinations are combined to generate the final beacon.

The resulting value becomes the distributed random output for the round.
