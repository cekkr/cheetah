# graph-nell demo

This demo uses `cheetah-db` graph commands against the NELL dataset:

- `studies/datasets/bkisiel_aaai10_08m.100.SSFeedback.csv`

It performs three things end-to-end:

1. Learns graph data by ingesting positive NELL edges through `GRAPH_EDGE_SET`.
2. Validates performance (ingest throughput + graph query latency).
3. Validates prediction quality from probability priors to implicit-correlation inference.

## What gets learned

- Node + edge graph built from NELL triples `(Relation, Entity, Value, Probability)`.
- Edge confidence is stored in `weight`.
- Relation priors, source-relation priors, relation co-occurrence conditionals, and relation-category compatibilities are learned from the training split.

## Metrics produced

- Ingest edges/sec.
- `GRAPH_NEIGHBOR_TYPES` latency p50/p95/p99.
- Prediction throughput.
- Probability model validity: `AUC`, `Average Precision`, `Precision@100`.
- Implicit-correlation model validity (base + reranked): `AUC`, `Average Precision`, `Precision@100`.
- Raw NELL probability validity vs action label (positive=`Action==""`, negative=`Action` starts with `-`).
- Automatic report artifacts (`JSON` per run + append-only `CSV`).

## Run

Start `cheetah-server` first (same host/port you pass below), then run:

```bash
go run ./demo/graph-nell \
  --host 127.0.0.1 \
  --port 4455 \
  --database graph_nell_demo \
  --reset-db \
  --dataset studies/datasets/bkisiel_aaai10_08m.100.SSFeedback.csv \
  --min-prob 0.70 \
  --holdout 0.10 \
  --eval-positive-limit 3000 \
  --eval-negatives 1 \
  --ingest-batch-size 128 \
  --skip-ingest=false \
  --implicit-rerank-topk 1000 \
  --implicit-rerank-alpha 0.40 \
  --write-reports \
  --report-dir demo/graph-nell/reports \
  --query-bench 1500
```

For a faster smoke run:

```bash
go run ./demo/graph-nell --max-rows 50000 --max-ingest-edges 30000 --query-bench 300
```

## Important notes

- The loader keeps positive edges where `Action` is empty and `Probability >= --min-prob`.
- Holdout is applied only to non-`generalizations` relations so category knowledge stays available for implicit predictions.
- Ingestion uses `GRAPH_EDGE_SET_BATCH` by default for lower TCP overhead; set `--ingest-batch-size 1` to force single-edge writes.
- For fast reranker sweeps on an already loaded graph, run with `--skip-ingest` and `--reset-db=false`.
- The implicit model combines:
  - relation co-occurrence around the source node,
  - destination category compatibility,
  - relation/source probability priors.
- Stage-2 reranking blends top-K implicit scores with probability priors to raise head precision (`P@100`) without discarding graph-context signals globally.
