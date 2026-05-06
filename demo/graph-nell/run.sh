#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

go run ./demo/graph-nell \
  --host "${CHEETAH_HOST:-127.0.0.1}" \
  --port "${CHEETAH_PORT:-4455}" \
  --database "${CHEETAH_GRAPH_DB:-graph_nell_demo}" \
  --reset-db \
  --dataset "${NELL_DATASET_PATH:-studies/datasets/bkisiel_aaai10_08m.100.SSFeedback.csv}" \
  --min-prob "${NELL_MIN_PROB:-0.70}" \
  --holdout "${NELL_HOLDOUT:-0.10}" \
  --eval-positive-limit "${NELL_EVAL_POS_LIMIT:-3000}" \
  --eval-negatives "${NELL_EVAL_NEGATIVES:-1}" \
  --query-bench "${NELL_QUERY_BENCH:-1500}" \
  "$@"
