// Package chaos runs a real three-node cluster (separate processes, real fsync, real TCP) and
// attacks it while five clients use it: kill -9 of random nodes and of the leader, and SIGSTOP of
// the leader so it wakes up still believing it leads. Every client operation is recorded and the
// history is checked for linearizability with the same Porcupine model the simulator uses.
//
//	go test ./chaos -chaos -v                       # 60 s of faults
//	go test ./chaos -chaos -chaos.duration 5m -v
//
// Unlike the simulator this is not deterministic: it is the check that the real binary, its
// storage and its transport keep the guarantees the simulated node has.
package chaos

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Kage7047/faultline/client"
	"github.com/Kage7047/faultline/kv"
	"github.com/Kage7047/faultline/server"
)

var (
	enabled  = flag.Bool("chaos", false, "run the chaos tests (they start real processes)")
	duration = flag.Duration("chaos.duration", 60*time.Second, "how long faults run")
	report   = flag.String("chaos.report", "", "write a JSON summary here")
	keep     = flag.String("chaos.dir", "", "keep logs, data and any failing history under this directory")
)

// TestKill9 is the real thing: the cluster must stay linearizable through every fault.
func TestKill9(t *testing.T) {
	r := run(t, *duration, false)
	if r.Violation != "" {
		t.Fatal(r.Violation)
	}
	if r.Ops < 200 {
		t.Fatalf("only %d operations completed", r.Ops)
	}
}

// TestControlStaleReads runs the same attack on binaries that answer reads without confirming
// leadership. The paused leader wakes up and serves reads from its old state before it hears
// about the new term. The checker must catch it, or a passing TestKill9 means little.
func TestControlStaleReads(t *testing.T) {
	r := run(t, *duration/2, true)
	if r.Violation == "" {
		t.Fatalf("the unsafe cluster survived %s of faults (%d ops, %d pauses)", *duration/2, r.Ops, r.Pauses)
	}
	t.Logf("caught: %s", r.Violation)
}

type summary struct {
	Duration   string `json:"duration"`
	Ops        int    `json:"ops"`
	Reads      int    `json:"reads"`
	Unknown    int    `json:"unknown_writes"` // writes whose answer never came back
	Attempts   int    `json:"attempts"`
	Kills      int    `json:"kills"`
	LeaderHits int    `json:"leader_kills_and_pauses"`
	Pauses     int    `json:"pauses"`
	MaxTerm    uint64 `json:"max_term"`
	Installs   int    `json:"snapshots_installed"`
	Violation  string `json:"violation,omitempty"`
}

type cluster struct {
	t     *testing.T
	bin   string
	dir   string
	addrs []string
	spec  string
	extra []string
	mu    sync.Mutex
	procs []*exec.Cmd
}

func run(t *testing.T, faults time.Duration, unsafe bool) summary {
	if !*enabled {
		t.Skip("starts real processes: run with -chaos")
	}
	c := newCluster(t, 3)
	if unsafe {
		c.extra = []string{"-unsafe-read-without-quorum"}
	}
	for i := range c.addrs {
		c.start(i)
	}
	t.Cleanup(c.stopAll)
	c.waitLeader(10 * time.Second)

	var (
		mu      sync.Mutex
		history []porcupine.Operation
		sum     summary
	)
	t0 := time.Now()
	now := func() int64 { return int64(time.Since(t0)) }
	stop := make(chan struct{})
	var clients sync.WaitGroup
	for id := range 5 {
		clients.Go(func() {
			// Impatient clients give up on a frozen node quickly and write through the new leader;
			// patient ones are still waiting on the old leader when it wakes up, and send it their
			// next request at once. That mix is what lets a stale read show up at all.
			attempt := 250 * time.Millisecond
			if id >= 3 {
				attempt = 5 * time.Second
			}
			cl := client.New(c.addrs, attempt)
			rng := rand.New(rand.NewPCG(uint64(id), uint64(t0.UnixNano())))
			for seq := 1; ; seq++ {
				select {
				case <-stop:
					mu.Lock()
					sum.Attempts += cl.Attempts
					mu.Unlock()
					return
				default:
				}
				in := kv.Input{Key: fmt.Sprintf("k%d", rng.IntN(3))}
				switch r := rng.Float64(); {
				case r < 0.4:
					in.Op = kv.Get
				case r < 0.7:
					in.Op, in.Value = kv.Put, fmt.Sprintf("%d.%d", id, seq)
				default:
					in.Op, in.Value = kv.Append, fmt.Sprintf("%d.%d;", id, seq)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				call := now()
				var out string
				var err error
				switch in.Op {
				case kv.Get:
					out, err = cl.Get(ctx, in.Key)
				case kv.Put:
					out, err = cl.Put(ctx, in.Key, in.Value)
				default:
					out, err = cl.Append(ctx, in.Key, in.Value)
				}
				ret := now()
				cancel()
				mu.Lock()
				switch {
				case err == nil:
					history = append(history, porcupine.Operation{ClientId: id, Call: call, Return: ret, Input: in,
						Output: kv.Output{Value: out, Known: true}})
					sum.Ops++
					if in.Op == kv.Get {
						sum.Reads++
					}
				case in.Op != kv.Get:
					// It may have happened, at any time after it started.
					history = append(history, porcupine.Operation{ClientId: id, Call: call, Return: math.MaxInt64,
						Input: in, Output: kv.Output{}})
					sum.Unknown++
				}
				mu.Unlock()
			}
		})
	}

	rng := rand.New(rand.NewPCG(uint64(t0.UnixNano()), 7))
	deadline := time.Now().Add(faults)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(500+rng.IntN(1500)) * time.Millisecond)
		victim, isLeader := rng.IntN(3), false
		if l := c.leader(); l >= 0 && (unsafe || rng.IntN(2) == 0) {
			victim, isLeader = l, true
		}
		if isLeader {
			sum.LeaderHits++
		}
		if unsafe || rng.IntN(3) == 0 {
			// Freeze it long enough for the others to elect a new leader and take writes, then let
			// it wake up believing it still leads.
			sum.Pauses++
			c.signal(victim, syscall.SIGSTOP)
			time.Sleep(time.Duration(400+rng.IntN(1200)) * time.Millisecond)
			c.signal(victim, syscall.SIGCONT)
		} else {
			sum.Kills++
			c.kill(victim)
			time.Sleep(time.Duration(200+rng.IntN(1500)) * time.Millisecond)
			c.start(victim)
		}
	}
	close(stop)
	clients.Wait()

	sts := c.converge(30 * time.Second)
	for _, st := range sts {
		sum.MaxTerm = max(sum.MaxTerm, uint64(st.Term))
		sum.Installs += st.Counters.Installs
	}
	sum.Duration = faults.String()

	res, info := porcupine.CheckOperationsVerbose(kv.Model, history, 2*time.Minute)
	switch res {
	case porcupine.Illegal:
		viz := filepath.Join(c.dir, "history.html")
		porcupine.VisualizePath(kv.Model, info, viz)
		sum.Violation = fmt.Sprintf("not linearizable: %d operations have no valid single-copy order (see %s)", len(history), viz)
	case porcupine.Unknown:
		t.Errorf("the checker could not decide on %d operations in time", len(history))
	}
	t.Logf("%+v", sum)
	if *report != "" && !unsafe {
		b, _ := json.MarshalIndent(sum, "", "  ")
		os.WriteFile(*report, b, 0o644)
	}
	return sum
}

func newCluster(t *testing.T, n int) *cluster {
	dir := t.TempDir()
	if *keep != "" {
		dir = filepath.Join(*keep, t.Name())
		os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	bin := filepath.Join(dir, "faultline")
	build := exec.Command("go", "build", "-o", bin, "../cmd/faultline")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	c := &cluster{t: t, bin: bin, dir: dir, procs: make([]*exec.Cmd, n)}
	var spec []string
	for i := range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		c.addrs = append(c.addrs, addr)
		spec = append(spec, fmt.Sprintf("%d=%s", i+1, addr))
	}
	c.spec = strings.Join(spec, ",")
	return c
}

func (c *cluster) start(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	logf, err := os.OpenFile(filepath.Join(c.dir, fmt.Sprintf("node%d.log", i+1)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		c.t.Fatal(err)
	}
	args := append([]string{"-id", fmt.Sprint(i + 1), "-cluster", c.spec, "-data", filepath.Join(c.dir, fmt.Sprint(i+1)),
		"-snapshot-every", "200"}, c.extra...)
	cmd := exec.Command(c.bin, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.procs[i] = cmd
}

func (c *cluster) kill(i int) {
	c.mu.Lock()
	p := c.procs[i]
	c.mu.Unlock()
	p.Process.Kill() // SIGKILL: no shutdown, no flush, nothing
	p.Wait()
}

func (c *cluster) signal(i int, sig syscall.Signal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.procs[i].Process.Signal(sig)
}

func (c *cluster) stopAll() {
	for i, p := range c.procs {
		if p != nil && p.ProcessState == nil {
			p.Process.Signal(syscall.SIGCONT)
			p.Process.Signal(syscall.SIGTERM)
			p.Wait()
		}
		c.procs[i] = nil
	}
}

func (c *cluster) status(i int) (server.Status, error) {
	hc := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := hc.Get("http://" + c.addrs[i] + "/status")
	if err != nil {
		return server.Status{}, err
	}
	defer resp.Body.Close()
	var st server.Status
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

// leader returns the index of the node that says it leads in the highest term, or -1.
func (c *cluster) leader() int {
	best, term := -1, uint64(0)
	for i := range c.addrs {
		if st, err := c.status(i); err == nil && st.Role == "leader" && uint64(st.Term) >= term {
			best, term = i, uint64(st.Term)
		}
	}
	return best
}

func (c *cluster) waitLeader(d time.Duration) {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if c.leader() >= 0 {
			return
		}
	}
	c.t.Fatal("no leader elected")
}

// converge waits until every node has applied the same index and holds the same state.
func (c *cluster) converge(d time.Duration) []server.Status {
	var sts []server.Status
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		sts = sts[:0]
		for i := range c.addrs {
			if st, err := c.status(i); err == nil {
				sts = append(sts, st)
			}
		}
		if len(sts) == len(c.addrs) && sts[0].Applied > 0 {
			same := true
			for _, st := range sts[1:] {
				same = same && st.Applied == sts[0].Applied && st.StateHash == sts[0].StateHash
			}
			if same {
				return sts
			}
		}
	}
	c.t.Fatalf("replicas did not converge: %+v", sts)
	return nil
}
