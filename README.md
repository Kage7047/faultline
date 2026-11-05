# faultline

[![ci](https://github.com/Kage7047/faultline/actions/workflows/ci.yml/badge.svg)](https://github.com/Kage7047/faultline/actions/workflows/ci.yml)
![go](https://img.shields.io/badge/go-1.27-00ADD8)
![license](https://img.shields.io/badge/license-MIT-green)

Raft consensus and a replicated key-value store in Go, built to be tested by a deterministic
simulator. **Work in progress**: consensus, the store, linearizability checking, PreVote and
CheckQuorum are done; snapshots and a real networked server are next (see [Roadmap](#roadmap)).

The Raft node is a pure state machine. It does no I/O, starts no goroutines and reads no clock:
the caller feeds it ticks and messages and collects what to persist, send and apply. That lets the
same code run under a simulator where **one seed decides everything**: message latency, loss,
duplication, reordering, partitions, crashes, restarts and election timeouts. A failure found on
seed 1565 replays on seed 1565, exactly, on any machine. CI and a laptop produce the same numbers
for the same seed, down to the message count.

## What gets checked

The simulated cluster has 5 nodes. Messages take 1 to 30 ms (1% of them ten times longer, so they
arrive after newer ones), 5% are lost and 2% duplicated, a node crashes about every 1.7 s and the
network splits at random about every 2.5 s. Every seed runs for 30 virtual seconds.

**Consensus** (`TestSafety`, 200 seeds per mode). After every step the checker tests the safety
properties from Figure 3 of the [Raft paper](https://raft.github.io/raft.pdf): one leader per term,
every node applies the same entry at each index and in order, every new leader holds every
committed entry, and the final logs match. The simulation alone executes 88.6% of the `raft`
package's statements; the rest are getters and `String` methods.

**The key-value store** (`TestLinearizable`, 100 seeds per mode). Five clients put, append and get
on three keys. They don't know who leads, follow "not leader" hints, time out and retry the same
request against another node, and their requests and answers cross the same lossy network. Every
run's history (over 1,000 operations) goes to [Porcupine](https://github.com/anishathalye/porcupine),
which searches for a single-copy order consistent with every result each client saw.

| | elections | crashes | partitions | messages | client ops | violations |
|---|---:|---:|---:|---:|---:|---:|
| consensus, PreVote on | 1,320 | 2,664 | 1,208 | 3,069,691 | | **0** |
| consensus, classic | 2,853 | 2,669 | 1,233 | 2,928,685 | | **0** |
| key-value, PreVote on | 648 | 1,302 | 599 | 6,190,094 | 127,063 | **0** |
| key-value, classic | 1,114 | 1,346 | 603 | 5,571,790 | 113,296 | **0** |

CI also runs 2,000 consensus seeds and 300 key-value seeds per mode that no run has used before,
on every push.

## The controls

A passing run only means something if broken code fails it. Every safety rule has a switch that
turns it off, and a test that requires the simulator to catch the result:

| rule switched off | caught on | what the checker saw |
|---|---|---|
| vote only for a candidate whose log is at least as up to date (§5.4.1) | seed 4 | a leader of term 6 missing a committed entry |
| commit only entries from the current term (§5.4.2, the Figure 8 bug) | seed 1,565 | a leader of term 64 missing a committed entry |
| deduplicate retried requests by (client, seq) | seed 3 | no linearizable order: a retried append applied twice |
| reads go through the log (a leader may not answer from local state) | seed 3 | no linearizable order: a cut-off leader served a stale value |

Two of these are worth a closer look.

**The Figure 8 bug survived 1,564 hostile runs.** A new leader's no-op travels in the same message
as the old entries before it, so an old-term entry almost never reaches a majority on its own.
The control has to send one entry per message before the window opens at all (the same setting
with the rule on stays safe).

**Good features hide bugs from tests.** PreVote and CheckQuorum halve the number of elections and
cut term growth by 90% on the same seeds (`TestPreVoteReducesElections`), which is what you want
in production. With them on, the Figure 8 control survived 2,000 seeds and the stale-read control
200: the windows those bugs need almost never open. The bugs are still there, so every safety test
runs in both modes, and those two controls run with both features off.

## PreVote and CheckQuorum

Without them, a node cut off from the cluster keeps timing out and bumping its term; when it comes
back, its higher term forces a healthy leader to step down. `TestReturningNodeAndPreVote` shows it
directly: after 200 ticks alone, the returning node drags the leader from term 1 to term 14.
With PreVote it asks whether it could win before bumping anything, and with CheckQuorum the
others ignore it while they still hear from a leader; the leader stays at term 1. CheckQuorum also
makes a leader that cannot reach a majority step down instead of believing it leads forever.

## Layout

```
raft/   the consensus algorithm: elections, replication, commit, PreVote, CheckQuorum, restart
kv/     the replicated state machine: get/put/append with client sessions (exactly-once retries)
sim/    the deterministic simulator, fault model, clients, safety checker and Porcupine model
```

## Running it

```bash
go test ./...                                  # everything, both modes, the controls (~2 min)
go test ./sim -run TestSafety -seeds 5000 -v   # more seeds
go test ./sim -run TestSafety -seed 1565 -v    # replay one seed
go test -race -short ./...
```

## Roadmap

- [x] Raft core: elections, log replication with conflict hints, commit rule, persistence and restart
- [x] Deterministic simulator with crashes, partitions, loss, duplication, reordering
- [x] Safety checker and controls that prove it catches broken rules
- [x] Key-value state machine with client sessions (exactly-once writes across retries)
- [x] Linearizability checking of client histories (Porcupine) in every simulated run
- [x] PreVote and CheckQuorum, and a test showing why: a node back from a partition deposing a healthy leader
- [ ] ReadIndex reads (linearizable without a log write), with the stale-read control kept honest
- [ ] Snapshots and log compaction
- [ ] A real server: networked transport, an HTTP API, a local 3-node cluster you can `kill -9`
- [ ] A write-up of what the simulator found

## License

MIT
