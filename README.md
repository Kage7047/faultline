# faultline

[![ci](https://github.com/Kage7047/faultline/actions/workflows/ci.yml/badge.svg)](https://github.com/Kage7047/faultline/actions/workflows/ci.yml)
![go](https://img.shields.io/badge/go-1.27-00ADD8)
![license](https://img.shields.io/badge/license-MIT-green)

Raft consensus and a replicated key-value store in Go, tested two ways: by a deterministic
simulator that runs hundreds of hostile 30-second clusters per minute, and by a chaos test that
`kill -9`s and freezes real processes while clients use them. Both feed every client history to
the [Porcupine](https://github.com/anishathalye/porcupine) linearizability checker, and both have
controls: deliberately broken builds that the checks must catch.

The Raft node is a pure state machine. It does no I/O, starts no goroutines and reads no clock:
the caller feeds it ticks and messages and collects what to persist, send and apply. That lets the
same code run under a simulator where **one seed decides everything**: message latency, loss,
duplication, reordering, partitions, crashes, restarts and election timeouts. A failure found on
seed 1565 replays on seed 1565, exactly, on any machine. CI and a laptop produce the same numbers
for the same seed, down to the message count. The same node runs in the real server, behind a
write-ahead log and an HTTP transport.

**Write-up:** [Every feature I added to my Raft hid a bug from my tests](https://kage7047.github.io/writing/features-hide-bugs/).
Prebuilt binaries for Linux and macOS, with build provenance, are on the
[releases page](https://github.com/Kage7047/faultline/releases).

## Try it

```bash
scripts/cluster.sh up                                   # nodes 1-3 on ports 7001-7003
curl -L -X PUT --data-binary hello localhost:7001/kv/greeting
curl -L -X POST --data-binary ' world' 'localhost:7002/kv/greeting?op=append'
curl -L localhost:7003/kv/greeting                      # hello world
scripts/cluster.sh status                               # who leads, terms, commit indexes
scripts/cluster.sh kill9 2                              # kill whichever node leads
curl -L localhost:7001/kv/greeting                      # a new leader answers
scripts/cluster.sh start 2                              # it recovers from its WAL and catches up
curl localhost:7001/metrics                             # Prometheus text format
scripts/cluster.sh down
```

Followers answer with a `307` to the leader (`-L` follows it). Writes sent with
`Faultline-Client` and `Faultline-Seq` headers are applied exactly once however often they are
retried; the Go client in `client/` does that for you.

## What the simulator checks

The simulated cluster has 5 nodes. Messages take 1 to 30 ms (1% of them ten times longer, so they
arrive after newer ones), 5% are lost and 2% duplicated, a node crashes about every 1.7 s and the
network splits at random about every 2.5 s. Every node snapshots its state and drops its log
every 50 applied entries, so a node back from a crash often needs a snapshot from the leader.
Every seed runs for 30 virtual seconds.

**Consensus** (`TestSafety`, 200 seeds per mode). After every step the checker tests the safety
properties from Figure 3 of the [Raft paper](https://raft.github.io/raft.pdf): one leader per term,
every node applies the same entry at each index and in order, every new leader holds every
committed entry, and the final logs match. Every installed snapshot is checked against a hash
chain of the committed log, and replicas that applied the same prefix must hold the same state.

**The key-value store** (`TestLinearizable`, 100 seeds per mode). Five clients put, append and get
on three keys. They don't know who leads, follow "not leader" hints, time out and retry the same
request against another node, and their requests and answers cross the same lossy network. Every
run's history (over 1,000 operations) goes to [Porcupine](https://github.com/anishathalye/porcupine),
which searches for a single-copy order consistent with every result each client saw.

Every test runs in three modes, because each optional feature hides some bugs (see below):
everything on; PreVote and CheckQuorum off (classic); and snapshots off as well (bare).

| | elections | crashes | partitions | messages | snapshots installed | client ops | violations |
|---|---:|---:|---:|---:|---:|---:|---:|
| consensus, everything on | 1,354 | 2,688 | 1,211 | 3,057,437 | 956 | | **0** |
| consensus, classic | 2,820 | 2,664 | 1,220 | 2,916,771 | 903 | | **0** |
| consensus, bare | 2,853 | 2,669 | 1,233 | 2,928,685 | | | **0** |
| key-value, everything on | 624 | 1,331 | 567 | 3,979,402 | 830 | 125,070 | **0** |
| key-value, classic | 1,214 | 1,327 | 583 | 3,612,252 | 772 | 109,700 | **0** |
| key-value, bare | 1,260 | 1,341 | 602 | 3,566,815 | | 108,200 | **0** |

CI also runs 2,000 consensus seeds and 300 key-value seeds per mode that no run has used before,
on every push.

## The controls

A passing run only means something if broken code fails it. Every safety rule has a switch that
turns it off, and a test that requires the simulator to catch the result (the table shows the lowest
seed that does, which is the same on every machine):

| rule switched off | caught on | what the checker saw |
|---|---|---|
| vote only for a candidate whose log is at least as up to date (§5.4.1) | seed 1 | a leader of term 7 missing a committed entry |
| commit only entries from the current term (§5.4.2, the Figure 8 bug) | seed 1,565 | a leader of term 64 missing a committed entry |
| ignore an InstallSnapshot older than what is already committed | seed 1 | a follower rolled back from applied index 88 to a snapshot at 50 |
| deduplicate retried requests by (client, seq) | seed 1 | no linearizable order: a retried append applied twice |
| snapshots include the dedup table | seed 30 | no linearizable order: a replica restored from a snapshot applied a retry again |
| confirm leadership with a majority before answering a read (ReadIndex) | seed 1 | no linearizable order: a cut-off leader served a stale value |
| a new leader serves reads only after committing an entry of its own term | seed 2 | no linearizable order: a read missed a write that had already completed |

Three of these are worth a closer look.

**The Figure 8 bug survived 1,564 hostile runs.** A new leader's no-op travels in the same message
as the old entries before it, so an old-term entry almost never reaches a majority on its own.
The control has to send one entry per message before the window opens at all (the same setting
with the rule on stays safe).

**Good features hide bugs from tests.** PreVote and CheckQuorum halve the number of elections and
cut term growth by 90% on the same seeds (`TestPreVoteReducesElections`), which is what you want
in production. With them on, the Figure 8 control survived 2,000 seeds and the stale-read control
200: the windows those bugs need almost never open. Snapshots did the same thing again: with
compaction on, the Figure 8 control survived all 2,000 seeds, because a lagging follower now
catches up with one snapshot instead of entry by entry. The bugs are still there, so every safety
test runs in all three modes, and each control runs in the mode where its window is open.
`TestControlMatrix` (`-matrix`, about 15 minutes) runs every control in every mode:

| rule switched off | everything on | classic | bare |
|---|---|---|---|
| vote for any candidate | seed 1 | seed 1 | seed 1 |
| commit old-term entries (one entry per message) | **missed in 2,000** | **missed in 2,000** | seed 1,565 |
| install a stale snapshot | seed 1 | seed 1 | n/a |
| no dedup of retried writes | seed 1 | seed 1 | seed 1 |
| snapshot without the dedup table | seed 30 | seed 4 | n/a |
| read without the quorum round | **missed in 200** | seed 1 | seed 3 |
| read before the term's first commit (one entry per message) | seed 11 | seed 2 | seed 39 |

No single mode catches all seven, and the one you would deploy misses two.

**A bug hidden by the code next to it.** A new leader does not know the cluster's commit index
until its own no-op commits; answering a read before then can miss a write the old leader already
confirmed. But the heartbeat round that confirms the read usually carries the no-op as well, so
the commit index catches up in the same step and the read comes out fresh. 200 seeds in both
modes missed it. With one entry per message, a lagging follower confirms the read before it has
the no-op, and seed 2 shows the stale read. The same setting with the rule on stays
linearizable (`TestOneEntryPerMessageIsLinearizable`).

## ReadIndex reads

A read does not need a log entry; it needs proof that no newer leader exists (§6.4 of Ongaro's
thesis). The leader notes its commit index, and every heartbeat carries a read sequence number
that followers echo back. Once a majority has echoed it, nobody else can have committed anything
the leader does not know, and the read is served as soon as the local store has applied that
index. On the same 50 seeds (`TestReadIndexSavesLogWrites`) log entries per operation drop from
1.06 to 0.64, and the history stays linearizable.

## PreVote and CheckQuorum

Without them, a node cut off from the cluster keeps timing out and bumping its term; when it comes
back, its higher term forces a healthy leader to step down. `TestReturningNodeAndPreVote` shows it
directly: after 200 ticks alone, the returning node drags the leader from term 1 to term 14.
With PreVote it asks whether it could win before bumping anything, and with CheckQuorum the
others ignore it while they still hear from a leader; the leader stays at term 1. CheckQuorum also
makes a leader that cannot reach a majority step down instead of believing it leads forever.

## Snapshots

Each node snapshots on its own once it has applied 50 entries past its last snapshot, then drops
those entries. A follower that needs an entry the leader no longer has gets the leader's snapshot
instead (InstallSnapshot, Figure 13 of the Raft paper); one that already holds the snapshot's
last entry keeps the log after it. A late or duplicated snapshot older than what the follower
committed is ignored. The store's snapshot holds the data and the session table, because without
the table a restored replica would apply a retried write twice. That was a classic bug in MIT's
6.824 labs, and the control above catches it.

## The chaos test: real processes

`go test ./chaos -chaos` builds the binary, starts three nodes (separate processes, real fsync,
real TCP on localhost) with five clients on them, and for 60 seconds every 0.5 to 2 seconds
either `kill -9`s a node (half the time the leader) and restarts it, or freezes the leader with
`SIGSTOP` for up to 1.6 s and wakes it with `SIGCONT`, still believing it leads. Then it waits
until every replica has applied the same index and holds the same state hash, and checks the
history with the same Porcupine model the simulator uses. It runs in CI on every push.

| run | client ops | `kill -9` | leader freezes | highest term | snapshots installed | violations |
|---|---:|---:|---:|---:|---:|---:|
| `TestKill9`, 60 s | 8,979 | 15 | 13 | 18 | 4 | **0** |
| `TestControlStaleReads`, 30 s, reads skip the quorum check | 4,451 | 0 | 15 | 16 | 2 | **caught** |

**The first version of the control passed, and that was the test's bug.** Every client waited
2 s per attempt, longer than any freeze, so while the leader was frozen all five clients were
stuck waiting on it, nobody wrote, and there was nothing stale to read when it woke up. Now three
clients give up after 250 ms and write through the new leader, and two keep waiting on the old
one, then send it their next read the moment it wakes, before it hears about the new term.
With that mix the broken build failed in all five runs so far, and the correct one passes.

What this does not test: `kill -9` kills the process, not the machine, so data the process wrote
is still in the OS page cache and survives without fsync. Power loss is covered at the file
level instead: the WAL tests cut the log at every byte offset of the last write and flip bits in
it, and every cut must reopen to a consistent state. Network partitions between real processes
(iptables or network namespaces) are not covered yet; the simulator covers them.

## Storage

Each node keeps a write-ahead log and a snapshot file (`wal/`). A log record is length, CRC-32C,
type and payload. A batch of hard state and entries is one write and one fsync, and the node
sends nothing that depends on it until that returns. On startup, the first record that is short
or fails its checksum is treated as a torn tail and cut off (and reported in
`faultline_wal_repaired_bytes`). Snapshots are written to a temporary file, fsynced, renamed into
place and the directory fsynced; the log is then rewritten without the entries the snapshot
covers. A crash between those two steps leaves a new snapshot and an old log, and replay skips
what the snapshot covers (`TestCrashBetweenSnapshotAndLogRewrite`).

## Security

Raft traffic can be authenticated with a cluster secret (`FAULTLINE_SECRET` or `-secret-file`):
every batch carries an HMAC-SHA256 of its body, and unsigned or wrongly signed batches get a
`401`. The chaos test runs with it on. Messages from nodes outside the membership are dropped
by the transport and again by `raft.Node.Step`, so they never count towards a quorum. Replay is
not prevented, on purpose: Raft already has to survive duplicated and delayed messages, which
is what a replay is, and the simulator tests exactly that. A snapshot from the leader is
decoded before it is written to disk, so a bad one stops the node instead of poisoning every
restart. Unsigned requests are refused before their body is read, at most a few
`/raft` requests run at once, and each must arrive within two seconds. Not done: TLS or mTLS
(the HMAC authenticates but does not encrypt), a separate peer-only listener, and any
authentication on the client API. Keep the ports on a private network, or put a proxy in front.

## Layout

```
raft/          the consensus algorithm: elections, replication, commit, PreVote, CheckQuorum,
               ReadIndex, snapshots, restart. No I/O.
kv/            the replicated state machine: get/put/append with client sessions (exactly-once
               retries), snapshots, and the Porcupine model of what correct means
sim/           the deterministic simulator, fault model, clients and safety checker
wal/           durable storage: CRC'd write-ahead log, torn-tail repair, atomic snapshots
server/        one node over a real network: the node loop, HTTP transport, client API, metrics
client/        a Go client that follows the leader and retries writes exactly once
cmd/faultline/ the binary
chaos/         the real-process chaos test
```

## Running it

```bash
go test ./...                                  # everything, three modes, the controls (~4 min)
go test ./sim -run TestSafety -seeds 5000 -v   # more seeds
go test ./sim -run TestSafety -seed 1565 -v    # replay one seed
go test -race -short ./...
go test ./chaos -chaos -v                      # the real-process chaos test (~2 min)
go test ./sim -run TestControlMatrix -matrix -v -timeout 60m   # every control in every mode
```

## Roadmap

- [x] Raft core: elections, log replication with conflict hints, commit rule, persistence and restart
- [x] Deterministic simulator with crashes, partitions, loss, duplication, reordering
- [x] Safety checker and controls that prove it catches broken rules
- [x] Key-value state machine with client sessions (exactly-once writes across retries)
- [x] Linearizability checking of client histories (Porcupine) in every simulated run
- [x] PreVote and CheckQuorum, and a test showing why: a node back from a partition deposing a healthy leader
- [x] ReadIndex reads (linearizable without a log write), with controls for both of its rules
- [x] Snapshots and log compaction, InstallSnapshot, and two controls (stale snapshot, missing dedup table)
- [x] A real server: WAL with fsync, HTTP transport and API, metrics, a local 3-node cluster you can `kill -9`
- [x] A chaos test of real processes (`kill -9`, `SIGSTOP`) checked by Porcupine, with a control
- [x] A write-up: [Every feature I added to my Raft hid a bug from my tests](https://kage7047.github.io/writing/features-hide-bugs/)
- [ ] Membership changes (joint consensus or one server at a time) and leader transfer
- [ ] Network partitions between real processes in the chaos test

## License

MIT
