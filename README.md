# faultline

[![ci](https://github.com/Kage7047/faultline/actions/workflows/ci.yml/badge.svg)](https://github.com/Kage7047/faultline/actions/workflows/ci.yml)
![go](https://img.shields.io/badge/go-1.27-00ADD8)
![license](https://img.shields.io/badge/license-MIT-green)

Raft consensus in Go, built to be tested by a deterministic simulator. **Work in progress**: the
consensus core and the simulator are done; the key-value store, linearizability checking and a
real networked server are next (see [Roadmap](#roadmap)).

The Raft node is a pure state machine. It does no I/O, starts no goroutines and reads no clock:
the caller feeds it ticks and messages and collects what to persist, send and apply. That lets the
same code run under a simulator where **one seed decides everything**: message latency, loss,
duplication, reordering, partitions, crashes, restarts and election timeouts. A failure found on
seed 1565 replays on seed 1565, exactly, on any machine.

## What the simulator found so far

`go test ./sim` runs 200 seeds of 30 virtual seconds each, on a 5-node cluster where messages take
1 to 30 ms (1% of them 10× longer), 5% are lost and 2% duplicated, a node crashes about every 1.7 s
and the network splits at random about every 2.5 s. One local run, in 3.4 s of real time:

| elections | crashes | partitions | messages | lost | safety violations |
|---:|---:|---:|---:|---:|---:|
| 2,853 | 2,669 | 1,233 | 2,928,685 | 471,589 | **0** |

After every step it checks the safety properties from Figure 3 of the
[Raft paper](https://raft.github.io/raft.pdf): one leader per term, every node applies the same
entry at each index in order, every new leader holds every committed entry, and the final logs
match. The simulation alone executes 88.6% of the `raft` package's statements (the rest are
getters and `String` methods).

**The controls.** A passing run only means something if broken Raft fails it, so each safety rule
has a switch that turns it off, and a test that requires the simulator to catch the result:

| rule switched off | caught on | what the checker saw |
|---|---|---|
| vote only for a candidate whose log is at least as up to date (§5.4.1) | seed 2 | a leader of term 14 missing a committed entry |
| commit only entries from the current term (§5.4.2, the Figure 8 bug) | seed 1,565 | a leader of term 64 missing a committed entry |

The second one is the interesting one. With the rule off, the bug survived 1,564 hostile runs,
because a new leader's no-op travels in the same message as the old entries before it, so an
old-term entry almost never reaches a majority on its own. The control has to send one entry per
message before the window opens at all (and the same setting with the rule on stays safe). A bug
that rare is exactly the kind a handful of hand-written tests never meets.

## Layout

```
raft/   the consensus algorithm: elections, replication, commit, restart from persisted state
sim/    the deterministic simulator, the fault model and the safety checker
```

## Running it

```bash
go test ./...                                  # unit tests, 200 seeds, the controls (~45 s)
go test ./sim -run TestSafety -seeds 5000 -v   # more seeds
go test ./sim -run TestSafety -seed 1565 -v    # replay one seed
go test -race -short ./...
```

## Roadmap

- [x] Raft core: elections, log replication with conflict hints, commit rule, persistence and restart
- [x] Deterministic simulator with crashes, partitions, loss, duplication, reordering
- [x] Safety checker and controls that prove it catches broken rules
- [ ] Key-value state machine with client sessions (exactly-once writes across retries)
- [ ] Linearizability checking of client histories (Porcupine) in every simulated run
- [ ] PreVote and CheckQuorum, and a test showing why: a node back from a partition deposing a healthy leader
- [ ] Snapshots and log compaction
- [ ] A real server: networked transport, an HTTP API, a local 3-node cluster you can `kill -9`
- [ ] A write-up of what the simulator found

## License

MIT
