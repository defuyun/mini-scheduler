#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

docker compose up -d

echo
echo "etcd:           http://localhost:2379"
echo "etcd-workbench: http://localhost:8002"
echo
echo "In the workbench UI, add a new connection with:"
echo "  Host: etcd"
echo "  Port: 2379"
echo "(use service name 'etcd' since workbench connects over the compose network)"
