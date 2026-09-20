# AGENTS.md

## Mission
Xây package Python 3.12 tên `hiereb` để replay dữ liệu smart-plug offline và đánh giá error-budget allocation.

## Stack bắt buộc
- Python 3.12, `uv`
- `polars`, `numpy`, `scipy`
- `pydantic`, `PyYAML`, `typer`
- `matplotlib`
- `pytest`, `hypothesis`
- `ruff`, `mypy`

## Scope
Có:
- CSV/Parquet input, lọc `house_id`
- warm-up/validation/evaluation
- time-slice median predictor
- event-time replay theo batch timestamp
- active/inactive/reactivation
- allocators: `full_tx`, `uniform`, `flat_variance`, `legacy_two_stage`, `true_hierarchical`, `true_hierarchical_cap`
- exact residual và uniform-censor proxy
- metrics, diagnostics, CSV/Parquet/JSON artifacts
- CLI + tests

Không có:
- Kafka, TimescaleDB, Grafana, Docker, REST/service
- neural network, Bayesian, GNN, RL

## Rules
- Source layout `src/hiereb`.
- Public API có type hints.
- Không global mutable state.
- Không dùng wall-clock trong thuật toán.
- Stable sort `(timestamp, household_id, plug_id, original_row_index)`.
- Event cùng timestamp là một batch nguyên tử.
- Threshold sinh tại `B` chỉ áp dụng cho `timestamp > B`.
- Không future leakage.
- Seed mặc định 42.
- Fail fast khi config sai.
- Không TODO trong main path.
- Mọi allocator dùng chung interface.
- Code math phải tham chiếu `docs/MATH_SPEC.md`.

## Commands bắt buộc
```bash
uv sync
uv run ruff check .
uv run mypy src
uv run pytest
uv run hiereb validate-config --config configs/house_0.example.yaml
uv run hiereb generate-synthetic --output data/synthetic_house_0.csv
uv run hiereb compare --config configs/house_0.example.yaml
uv run hiereb verify-legacy-equivalence --config configs/house_0.example.yaml
```

Chỉ hoàn thành khi mọi tiêu chí trong `docs/ACCEPTANCE_CRITERIA.md` pass.
