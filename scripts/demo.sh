#!/usr/bin/env bash
# End-to-end demo with a hard crash in the middle. Proves exactly-once delivery into silver.
# Prereqs: `docker compose up -d` (Kafka running) and the Python venv activated.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PYTHON:-python}
LOG=data/stream.log
mkdir -p data

start_stream() { nohup $PY -m telemetry.cli stream >> "$LOG" 2>&1 & echo $! > data/stream.pid; }
wait_table()   { until [ -d "lake/$1/_delta_log" ]; do sleep 2; done; }

echo "== 1. clean slate: delete lake, recreate topic"
$PY -m telemetry.cli reset
$PY -m telemetry.cli delete-topic || true
sleep 3
$PY -m telemetry.cli create-topic --partitions 6
: > "$LOG"

echo "== 2. start the streaming app (bronze -> silver + dead letter -> gold)"
start_stream
wait_table bronze/telemetry_raw

echo "== 3. wave 1: whole NASA fleet (200 engines) with chaos: 5% duplicates, 3% late by 30 s, 1% bad"
$PY -m telemetry.cli produce --rate 3000 --chaos --start-loop 0 &
PRODUCER=$!
sleep 12
echo "== 4. CRASH: kill -9 the streaming app while data is flowing"
kill -9 "$(cat data/stream.pid)" || true
sleep 5
echo "== 5. restart: checkpoints resume exactly where each query stopped"
start_stream
wait $PRODUCER

echo "== 6. wave 2: 40 new engines at 100 events/s for ~2.5 min, so the watermark advances and minute windows close"
$PY -m telemetry.cli produce --rate 100 --engines 20 --max-events 15000 --start-loop 1

echo "== 7. wait for the pipeline to drain, then verify exactly-once"
for i in $(seq 1 60); do $PY -m telemetry.cli verify > /dev/null 2>&1 && break; sleep 5; done
$PY -m telemetry.cli verify
$PY -m telemetry.cli report
echo "Streaming app still running (pid $(cat data/stream.pid)). Dashboard: streamlit run dashboard/app.py"
