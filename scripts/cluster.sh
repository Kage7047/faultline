#!/usr/bin/env bash
# A local three-node cluster for poking at by hand.
#
#   scripts/cluster.sh up            build and start nodes 1-3 on ports 7001-7003
#   scripts/cluster.sh status        role, term, leader and commit of each node
#   scripts/cluster.sh kill9 <id>    SIGKILL one node (no shutdown, nothing flushed)
#   scripts/cluster.sh start <id>    start one node again; it recovers from its WAL
#   scripts/cluster.sh down          stop everything (data stays in .cluster/)
set -euo pipefail
cd "$(dirname "$0")/.."
DIR=.cluster
SPEC=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003

start() {
  ./"$DIR"/faultline -id "$1" -cluster "$SPEC" -data "$DIR/data/$1" >>"$DIR/node$1.log" 2>&1 &
  echo $! >"$DIR/node$1.pid"
  echo "node $1 started (pid $!, log $DIR/node$1.log)"
}

case "${1:-}" in
up)
  mkdir -p "$DIR"
  go build -o "$DIR/faultline" ./cmd/faultline
  for i in 1 2 3; do start "$i"; done
  ;;
start) start "$2" ;;
kill9)
  kill -9 "$(cat "$DIR/node$2.pid")" && rm "$DIR/node$2.pid"
  echo "node $2 killed"
  ;;
status)
  for i in 1 2 3; do
    curl -s --max-time 1 "127.0.0.1:700$i/status" || echo "{\"id\":$i,\"role\":\"down\"}"
  done
  ;;
down)
  for f in "$DIR"/node*.pid; do [ -e "$f" ] && kill "$(cat "$f")" 2>/dev/null; rm -f "$f"; done
  echo stopped
  ;;
*)
  sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
  ;;
esac
