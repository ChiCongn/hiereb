# HierEB Offline Research Simulator

`hiereb` is a deterministic Python 3.12 research simulator for replaying
smart-plug events and comparing house error-budget allocators. It implements
event-time timestamp batches, a warm-up-only time-slice median predictor,
active/inactive plug handling, exact and uniform-censor residual state, six
allocation modes, safety/tail diagnostics, and reproducible artifacts.

The project intentionally contains no Kafka, TimescaleDB, Grafana, Docker,
HTTP service, neural network, or hidden model-update channel.

## Setup and quality gates

```bash
uv sync
uv run ruff check .
uv run mypy src
uv run pytest
```

## Synthetic experiment

The checked-in example config targets the deterministic 12-hour fixture:

```bash
uv run hiereb validate-config --config configs/house_0.example.yaml
uv run hiereb generate-synthetic --output data/synthetic_house_0.csv
uv run hiereb compare --config configs/house_0.example.yaml
uv run hiereb verify-legacy-equivalence --config configs/house_0.example.yaml
uv run hiereb inspect-outliers --run-dir outputs/direction1-house0/true_hierarchical_cap
```

Each mode is written below `outputs/direction1-house0/<mode>/`; the root also
contains `comparison.csv`.

## DEBS CSV

Copy the example config and change only dataset/split/output settings. The
loader accepts the DEBS columns `id` (optional), `timestamp`, `value`,
`property`, `plug_id`, `household_id`, `house_id`; it defaults to load
`property == 1` and does not hard-code a house.

Enable bounded-memory execution for large CSV/Parquet files:

```yaml
data:
  path: "data/house-0.csv"
  format: "csv"
  house_id: 0
  load_property_value: 1
  timestamp_unit: "seconds"
  timezone: "UTC"
  streaming:
    enabled: true
    chunk_rows: 100000
    staging_dir: "data/.hiereb-staging"
    reuse_staging: true
  columns:
    id: "id"
    timestamp: "timestamp"
    value: "value"
    property: "property"
    plug_id: "plug_id"
    household_id: "household_id"
    house_id: "house_id"
```

The first run filters and externally stable-sorts the selected house/window
into a keyed Parquet staging file. Later modes and runs reuse that file when
the input hash and relevant data settings are unchanged. Equal-timestamp rows
are carried across chunk boundaries and replayed as one atomic batch.

```bash
cp configs/house_0.example.yaml configs/debs_house.yaml
# Edit data.path, data.house_id, warm-up/evaluation intervals, and output_dir.
uv run hiereb validate-config --config configs/debs_house.yaml
uv run hiereb run --config configs/debs_house.yaml --mode true_hierarchical_cap
uv run hiereb compare --config configs/debs_house.yaml
uv run hiereb verify-legacy-equivalence --config configs/debs_house.yaml
```

Checked-in house-0 research configs and path override:

```bash
export HIEREB_DATA_PATH=/absolute/path/to/house-0.csv
uv run hiereb validate-config --config configs/debs_house0_smoke.yaml
uv run hiereb experiment-house --config configs/debs_house0_smoke.yaml
uv run hiereb verify-determinism --config configs/debs_house0_smoke.yaml

# Equivalent explicit override:
uv run hiereb experiment-house \
  --config configs/debs_house0_evaluation.yaml \
  --data-path /absolute/path/to/house-0.csv
```

`experiment-house` prepares input/predictor/budget once, runs the six allocator
modes with invariant gates, writes hierarchy/cap/tail/outlier diagnostics,
performs OFAT validation when enabled, creates the nine-epsilon Pareto sweep,
compares exact versus proxy residual state, and generates
`HOUSE0_DIRECTION1_REPORT.md`.

For large inputs use `hiereb run`, `hiereb compare`,
`hiereb verify-legacy-equivalence`, or `hiereb verify-determinism` with
`data.streaming.enabled: true`. The streaming path does not retain raw
evaluation events: it keeps rolling state, compact numeric error arrays,
per-plug/household aggregates, top-K outliers, and threshold diagnostics.
`experiment-house` remains the richer eager sensitivity workflow and should
only be used with a window that fits memory.

For Parquet input set `data.format: parquet` and point `data.path` to the
Parquet file. Numeric timestamps follow `data.timestamp_unit`; textual
timestamps must be parseable as UTC.

## Timing and safety semantics

Events are stable-sorted by `(timestamp, household_id, plug_id,
original_row_index)`. Every equal-timestamp group is atomic: it uses the
threshold snapshot existing before the batch, then updates residual/active
state, and only then publishes an allocation whose `effective_after` equals
the batch timestamp. Suppression uses strict transmission semantics:
`abs(residual) > threshold` transmits, so equality suppresses.

The authoritative formulas and limitations are in
[`docs/MATH_SPEC.md`](docs/MATH_SPEC.md); implementation evidence is recorded
in [`IMPLEMENTATION_REPORT.md`](IMPLEMENTATION_REPORT.md).

## Direction 3 adaptive dual predictor

Generate the deterministic drift fixture and compare the five deployable
predictor modes while holding the allocator fixed:

```bash
uv run hiereb generate-direction3-synthetic --output data/synthetic_direction3.csv
uv run hiereb compare-predictors \
  --config configs/direction3_synthetic.yaml \
  --allocator uniform \
  --matched-tr
```

For a config-only matrix, declare allocator modes in `experiment.modes` and
predictor modes in `predictor.modes`, then omit `--allocator`:

```bash
uv run hiereb compare-predictors --config configs/all_modes.example.yaml
```

This supports six allocators (`full_tx`, `uniform`, `flat_variance`,
`legacy_two_stage`, `true_hierarchical`, `true_hierarchical_cap`) and the five
deployable predictor modes. Only the Cartesian product declared in the YAML is
run, while streaming preparation is reused once. Matrix summaries are written
to `predictor_matrix.csv` and `predictor_matrix.json`.

Per-pair results are written below
`<output_dir>/predictors/<allocator>/<predictor_mode>/`.

Tune on an enabled validation interval before the final DEBS evaluation:

```bash
uv run hiereb validate-predictor \
  --config configs/direction3_house0_validation.yaml \
  --data-path /absolute/path/to/house-0.csv \
  --allocator uniform
```

This runs the specified staged search (5 alpha candidates, 9 CUSUM
candidates, then count/time resynchronization OFAT), never reads the evaluation
interval for selection, and writes `selected_predictor_config.yaml` plus all
accepted/rejected candidates below
`<output_dir>/predictor-validation/<allocator>/`.

```bash
uv run hiereb inspect-drift \
  --run-dir outputs/direction3-synthetic/predictors/uniform/slot_median_ewma_drift
uv run hiereb inspect-resynchronization \
  --run-dir outputs/direction3-synthetic/predictors/uniform/slot_median_ewma_drift_periodic_sync
```

For DEBS, use one of the supplied smoke/validation/evaluation configs and the
existing data-path override:

```bash
uv run hiereb compare-predictors \
  --config configs/direction3_house0_evaluation.yaml \
  --data-path /absolute/path/to/house-0.csv \
  --allocator true_hierarchical_cap \
  --matched-tr
```

Drift and periodic synchronization are ordinary transmitted events included
in TR. Deployable modes require zero edge/server divergence. The local-oracle
mode is explicitly diagnostic and excluded from the primary comparison.
