# gossip-glomers

My solutions to Fly.io's [Gossip Glommers](https://fly.io/dist-sys/) problems in distributed systems.

This repository include solutions to challenges up to and including 5c, and my implementation for distributed nodes. My implementation emphasize flexibility, correctness, and a special attention to performance. It's not the simplest, but it is fairly powerful.

## Common
This crate contains messages common to all challenges, node implementations, ID handling and various other items necessary for the challenges.

## Runner
The runner crate is a simple CLI that tests the challenges against [Malestrom](https://github.com/jepsen-io/maelstrom), a local distributed system test framework. To run a given challenge, simply run
`cargo challenge <CHALLENGE>` in the workspace root. For example:
```
cargo challenge echo
```
or
```
cargo challenge kafka-b
```
This will build the appropriate challenge, run the tests and print metrics.

**NOTE:** You must have a `maelstrom` folder available in the workspace root. You can obtain the latest release from the repo linked above.

## Challenges
This folder contains a number of crates, each representing the solution to a Gossip Glommers challenge.
