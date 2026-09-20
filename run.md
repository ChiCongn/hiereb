# Run Experiment
## Config
`configs/offline_house0.yaml`
```yaml
experiment:
  name: "offline-house0"
  seed: 42
  output_dir: "outputs/offline-house0"
  overwrite: true

  # Dùng khi chạy không truyền --mode hoặc chạy compare.
  modes:
    - full_tx
    - uniform
    - true_hierarchical
    - true_hierarchical_cap

data:
  path: "data/house-0.csv"
  format: "csv"               # csv hoặc parquet
  house_id: 0
  load_property_value: 1
  timestamp_unit: "seconds"   # seconds, milliseconds, microseconds
  timezone: "UTC"

  columns:
    id: "id"                  # Có thể không tồn tại trong input
    timestamp: "timestamp"
    value: "value"
    property: "property"
    plug_id: "plug_id"
    household_id: "household_id"
    house_id: "house_id"

splits:
  warmup:
    start: "2013-09-01T00:00:00Z"
    end: "2013-09-01T23:59:59Z"

  validation:
    enabled: false
    start: null
    end: null

  evaluation:
    start: "2013-09-02T00:00:00Z"
    end: "2013-09-02T05:59:59Z"

predictor:
  type: "time_slice_median"
  slot_seconds: 300

budget:
  epsilon_ratio: 0.01

replay:
  allocation_period_seconds: 300
  active_window_seconds: 600
  float_tolerance: 1.0e-9
  top_k_outliers: 100

residual:
  estimator: "exact"          # exact hoặc uniform_proxy
  rolling_window_size: 1000
  min_samples: 30
  sigma_floor_percentile: 5.0

true_hierarchy:
  household_fairness_alpha: 0.20
  plug_fairness_beta: 0.10

  household_score:
    lambda_mean: 0.50
    lambda_max: 0.40
    lambda_count: 0.10

cap:
  enabled: true
  quantile: 0.95
  multiplier: 1.0
  max_house_budget_fraction: 0.25
  min_samples: 30
  redistribution_tolerance: 1.0e-9
  max_iterations_extra: 10

artifacts:
  write_threshold_trace: true
  write_plug_metrics: true
  write_household_metrics: true
  write_plots: true
```

## Prepare and validate
```bash
uv sync

uv run hiereb validate-config \
  --config configs/all_modes.example.yaml \
```

## Run mode
```bash
uv run hiereb run \
  --config configs/offline_house0.yaml \
  --mode true_hierarchical
```

```bash
uv run hiereb compare-predictors \
  --config configs/all_modes.example.yaml \
  --allocator uniform
```

Run all config mode
```bash
uv run hiereb run \
  --config configs/all_modes.example.yaml \
```

Thêm --matched-tr để chạy epsilon sweep:
```bash
uv run hiereb compare-predictors \
  --config configs/all_modes.example.yaml \
  --allocator true_hierarchical_cap \
  --matched-tr
```

## Output
- Tổng hợp: `outputs/offline-house0/comparison.csv`
- Xem outlier: 
```bash
uv run hiereb inspect-outliers \
  --run-dir outputs/offline-house0/true_hierarchical_cap \
  --limit 20
```

### Config
| File | Ý nghĩa |
|---|---|
| `resolved_config.yaml` | Cấu hình thực tế đã dùng cho run, sau khi áp dụng `--data-path` hoặc `HIEREB_DATA_PATH`. |
| `run_metadata.json` | Thông tin nhận dạng run: phiên bản package/Python, mode, seed, hash config và SHA-256 input. |
| `filter_counts.json` | Thống kê số dòng input được giữ hoặc loại theo từng lý do. |

### Kết quả tổng hợp từng mode
| File | Ý nghĩa |
|---|---|
| `summary.json` | Toàn bộ metric tổng hợp, giữ được object lồng nhau. |
| `summary.csv` | Cùng kết quả nhưng dạng một dòng CSV, tiện ghép nhiều run và dùng Excel/Polars. |


**Số lượng và transmission**
- valid_events: tổng event evaluation hợp lệ.
- transmitted: số event được truyền.
- suppressed: số event bị suppress.
- transmission_ratio: transmitted / valid_events.
- reduction: 1 - transmission_ratio.
- timestamp_batches: số batch timestamp.

### Phân rã kết quả
| File | Cấp phân tích |
|---|---|
| `daily_metrics.csv` | Theo ngày UTC |
| `household_metrics.csv` | Theo household |
| `plug_metrics.parquet` | Theo từng plug |
