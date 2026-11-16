#!/usr/bin/env bash
# 本机无 Docker 时的双实例等价启动脚本（自测/演示用）。
# 两个 uvicorn 进程共享同一数据目录，由 nginx 或自行用端口访问。
#
# 用法：
#   scripts/run_local.sh           # 启动 8001 / 8002 两实例
#   OBS_QUOTA_BYTES=10485760 scripts/run_local.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-$ROOT/.venv/bin/python}"
DATA_DIR="${OBS_DATA_DIR:-$ROOT/.local-data}"
QUOTA="${OBS_QUOTA_BYTES:-1073741824}"

mkdir -p "$DATA_DIR"
echo "data dir : $DATA_DIR"
echo "quota    : $QUOTA bytes"
echo "instance1: http://127.0.0.1:8001/docs"
echo "instance2: http://127.0.0.1:8002/docs"

OBS_DATA_DIR="$DATA_DIR" OBS_QUOTA_BYTES="$QUOTA" OBS_INSTANCE_ID=instance-1 \
  "$PY" -m uvicorn app.main:app --host 127.0.0.1 --port 8001 &
PID1=$!
OBS_DATA_DIR="$DATA_DIR" OBS_QUOTA_BYTES="$QUOTA" OBS_INSTANCE_ID=instance-2 \
  "$PY" -m uvicorn app.main:app --host 127.0.0.1 --port 8002 &
PID2=$!

trap 'kill $PID1 $PID2 2>/dev/null || true' INT TERM EXIT
wait
