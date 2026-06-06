# HierEB

Python-first Hierarchical Error Budget pipeline for DEBS smart-plug replay.

## Architecture

`simulator` replays DEBS CSV data into Kafka, `aggregator` writes house-level metrics to TimescaleDB, and `ml-hiereb` publishes time-slice predictions plus per-plug thresholds. Grafana reads TimescaleDB directly.

Core topics:

- `hiereb.sensor_data`
- `hiereb.predictions`
- `hiereb.thresholds`
- `hiereb.variance`

## Local Setup

```bash
cp .env.example .env
docker compose up -d kafka timescaledb grafana kafka-init
docker compose up simulator aggregator ml-hiereb
```

Use `SUPPRESSION_MODE=full_tx`, `uniform`, or `hiereb` in `.env`.

`EPSILON_H=0.05` means 5% of mean training house load. Use `EPSILON_H>=1` for an explicit Watt budget.

Set a unique `RUN_ID` per experiment when comparing modes on the same replay data.

## Tests

```bash
.venv/bin/python -m pytest -q
```

The unit tests do not require Kafka or TimescaleDB.

For a Docker smoke test of all three modes:

```bash
bash scripts/verify_e2e.sh
```

For a full demo flow (run 3 modes + export report CSV + generate report charts + keep Grafana up):

```bash
bash scripts/demo_report.sh
```

This writes artifacts to `results/demo_*/` and leaves the stack running by default:

- `e2e_summary.csv`
- `export/*.csv` (raw report data)
- `charts/*.svg` (ready to paste into slide/report)

## Sweep Runner

Generate a deterministic run plan without starting Docker:

```bash
bash scripts/run_sweep.sh --dry-run --sweep-id sweep01 --house-id 0
```

Default sweep epsilon ratios are `[0.01, 0.02, 0.05, 0.10, 0.20]`. If the
machine is resource-limited, use the reduced grid `[0.02, 0.05, 0.10]`:

```bash
bash scripts/run_sweep.sh --dry-run --reduced --sweep-id sweep01 --house-id 0
```

The plan always runs `full_tx` once with `epsilon_ratio=null`, then `uniform`
and `hiereb` once per epsilon. Epsilon run IDs use:

```text
{sweep_id}_{mode}_house{house_id}_eps{epsilon_ratio_x100}
```

Example: `sweep01_hiereb_house0_eps005`. The `full_tx` anchor run omits the
epsilon suffix, for example `sweep01_full_tx_house0`.

Run the sweep through the E2E verifier:

```bash
bash scripts/run_sweep.sh --execute --reduced --runtime-seconds 35 --sweep-id sweep01 --house-id 0
```

This writes `results/sweeps/<sweep_id>/sweep_plan.csv` with `sweep_id`,
`is_sweep`, `sweep_size`, and `reduced_sweep` metadata for every planned run.

## Data

CSV files are expected under `data/`, for example `data/house-1.csv`. Confirm the actual `house_id` before changing `HOUSE_IDS`:

```bash
head -2 data/house-1.csv | cut -d',' -f7
```

For multi-house runs, split a combined time window into one file per house:

```bash
python scripts/partition_debs_by_house.py \
  --input data/all-house/one-day.csv \
  --output-dir data/partitioned/one-day \
  --property 1
```

If the input is already split into full-history `house-*.csv` files, produce
a one-day partition set directly:

```bash
python scripts/partition_debs_by_house.py \
  --input data/house-*.csv \
  --output-dir data/partitioned/one-day \
  --property 1 --max-duration-seconds 86400 --skip-malformed --overwrite
```

The runtime then streams selected partitions rather than concatenating the
whole multi-house dataset in memory:

```bash
# One configured house
DATASET_PRESET=one_house DATA_WINDOW=one_day ONE_HOUSE_ID=1 bash scripts/verify_e2e.sh hiereb

# Five configured houses (use IDs present in your partitions)
DATASET_PRESET=five_houses DATA_WINDOW=one_day FIVE_HOUSE_IDS='[0,1,2,10,11]' bash scripts/verify_e2e.sh hiereb

# Every partition found in data/partitioned/one-day/
DATASET_PRESET=all_house DATA_WINDOW=one_day bash scripts/verify_e2e.sh hiereb
```

Use `STREAM_READ_CHUNK_SIZE` to tune replay memory per selected file; the
default is `10000` rows.

## Demo Dashboard

After `scripts/demo_report.sh`:

- Open `http://localhost:3000` (`admin` / `hiereb_pass`).
- Open dashboard `HierEB Report Dashboard`.
- Select `run_id` and `house_id` at the top to switch experiment results.

Main panels for report:

- `Actual vs Predicted Load`
- `House Error e_h`
- `Transmission Rate Trend`
- `Rolling RMSE (1 minute)`
- Stat cards: `Average TR`, `RMSE`, `P90 |e_h|`, `Violation Rate |e_h| > 50W`
- `Run Summary (All Houses)` table

Detailed Vietnamese demo and inspection guide:

```bash
less docs/demo_system_guide.md
```

Inspect the whole stack:

```bash
bash scripts/inspect_system.sh
```

Inspect every component:

```bash
LOG_LINES=80 bash scripts/inspect_all.sh
```

## Export CSV For Report

Export latest runs:

```bash
bash scripts/export_report_data.sh
```

Export specific runs:

```bash
bash scripts/export_report_data.sh <run_id_1> <run_id_2> <run_id_3>
```

Generate charts from exported CSV:

```bash
python3 scripts/generate_report_charts.py --input-dir results/demo_*/export --output-dir results/demo_*/charts
```
