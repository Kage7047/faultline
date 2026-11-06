// Command faultline runs one node of a faultline cluster.
//
//	faultline -id 1 -cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data data/1
//
// Each node serves both the raft transport and the client API on its own address:
//
//	curl -L -X PUT --data-binary hello localhost:7001/kv/greeting
//	curl -L localhost:7002/kv/greeting
//	curl localhost:7003/status
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Kage7047/faultline/raft"
	"github.com/Kage7047/faultline/server"
)

func main() {
	id := flag.Uint64("id", 0, "this node's id")
	cluster := flag.String("cluster", "", "every node as id=host:port, comma separated")
	data := flag.String("data", "", "data directory (default data/<id>)")
	tick := flag.Duration("tick", 10*time.Millisecond, "raft tick")
	snapEvery := flag.Int("snapshot-every", 1000, "snapshot after this many applied entries")
	secretFile := flag.String("secret-file", "",
		"file holding the cluster secret that authenticates raft traffic (default $FAULTLINE_SECRET)")
	unsafeReads := flag.Bool("unsafe-read-without-quorum", false,
		"CONTROL ONLY: answer reads without confirming leadership (serves stale reads)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	members, err := parseCluster(*cluster)
	if err != nil || *id == 0 {
		fmt.Fprintln(os.Stderr, "usage: faultline -id N -cluster 1=host:port,2=host:port,... [-data dir]")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(2)
	}
	if *data == "" {
		*data = fmt.Sprintf("data/%d", *id)
	}
	secret := []byte(os.Getenv("FAULTLINE_SECRET"))
	if *secretFile != "" {
		b, err := os.ReadFile(*secretFile)
		if err != nil {
			log.Error("read secret", "err", err)
			os.Exit(1)
		}
		secret = []byte(strings.TrimSpace(string(b)))
	}
	if len(secret) == 0 {
		secret = nil
		log.Warn("no cluster secret: anyone who can reach this port can send raft messages")
	}
	if *unsafeReads {
		log.Warn("UNSAFE: reads are answered without confirming leadership")
	}
	s, err := server.New(server.Config{
		ID:            raft.NodeID(*id),
		Cluster:       members,
		DataDir:       *data,
		Tick:          *tick,
		SnapshotEvery: *snapEvery,
		Log:           log,
		Secret:        secret,
		Unsafe:        raft.Unsafe{ReadWithoutQuorum: *unsafeReads},
	})
	if err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := s.Run(ctx); err != nil {
		log.Error("stopped", "err", err)
		os.Exit(1)
	}
}

func parseCluster(s string) (map[raft.NodeID]string, error) {
	m := map[raft.NodeID]string{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad cluster member %q", part)
		}
		n, err := strconv.ParseUint(k, 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("bad node id %q", k)
		}
		m[raft.NodeID(n)] = v
	}
	return m, nil
}
