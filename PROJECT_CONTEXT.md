# Hiereb - Tong quan codebase va context du an

> Tai lieu onboarding mot file, duoc tong hop tu source code, test, cau hinh va cac
> dac ta trong repo. Source code va `docs/MATH_SPEC.md` la nguon su that khi co
> mau thuan voi tai lieu cu.

## 1. Du an giai quyet bai toan gi?

`hiereb` la package Python 3.12 dung de replay offline du lieu smart-plug theo
event time va danh gia cach phan bo **error budget** cho cac plug trong mot nha.
Tai moi event, he thong du doan cong suat, so sanh residual voi threshold cua
plug, roi quyet dinh:

- **Transmit** neu sai so lon: server nhan gia tri that.
- **Suppress** neu sai so nam trong dead-band: server dung gia tri du doan de
  reconstruct.

Muc tieu nghien cuu la giam ty le truyen (TR) nhung van kiem soat reconstruction
error o cap house, dac biet la tail error, dong thoi so sanh allocator phang,
allocator phan cap, cap threshold va cac predictor co kha nang thich nghi drift.

Day la simulator nghien cuu deterministic, khong phai production service. Scope
co CSV/Parquet, replay, allocator, metrics, diagnostics, artifact va CLI. Scope
co chu y loai Kafka, TimescaleDB, Grafana, Docker, REST/HTTP, neural network,
Bayesian, GNN va reinforcement learning.

## 2. Hai huong thi nghiem trong code hien tai

### Direction 1: frozen predictor, so sanh allocator

Predictor time-slice median chi duoc fit tu warm-up va dong bang trong evaluation.
Sau do cung mot predictor va house budget duoc dung de so sanh sau allocator:

1. `full_tx`
2. `uniform`
3. `flat_variance`
4. `legacy_two_stage`
5. `true_hierarchical`
6. `true_hierarchical_cap`

`experiment-house` la workflow nghien cuu day du cho mot house: chay sau mode,
kiem tra invariant, phan tich hierarchy/cap/tail/outlier, OFAT validation, Pareto
sweep, exact-vs-proxy va sinh report.

### Direction 3: dual predictor thich nghi

Phan mo rong nay giu seasonal median dong bang nhung cong them EWMA bias, drift
detection va periodic resynchronization. Edge va server co state rieng de chung
minh deployability: suppressed actual khong duoc am tham cap nhat server state.

Nam mode deployable duoc so sanh:

1. `slot_median_frozen`
2. `slot_median_ewma`
3. `slot_median_ewma_periodic_sync`
4. `slot_median_ewma_drift`
5. `slot_median_ewma_drift_periodic_sync`

`slot_median_ewma_local_oracle` chi la diagnostic oracle, khong deployable va
khong nam trong primary comparison.

## 3. Stack va quy uoc bat buoc

- Runtime/package: Python 3.12, `uv`, source layout `src/hiereb`.
- Data/math: Polars, NumPy, SciPy.
- Config/CLI: Pydantic v2, PyYAML, Typer.
- Plot: Matplotlib.
- Quality: pytest, Hypothesis, Ruff, mypy strict.
- Seed mac dinh: `42`.
- Public API co type hints; config model frozen va `extra="forbid"`.
- Khong co global mutable state, khong dung wall-clock trong thuat toan.
- Moi allocator di qua mot interface chung `AllocationRequest -> AllocationResult`.

## 4. Mo hinh toan cot loi

Voi plug `p` tai timestamp `t`:

```text
r_p(t) = x_p(t) - xhat_p(t)
transmit iff abs(r_p(t)) > delta_p(t)
```

Phep so sanh la strict `>`: neu `abs(r) == delta` thi event bi suppress. Khi
transmit, reconstruction la actual; khi suppress, reconstruction la prediction.

House error duoc tinh tren cac plug co valid event trong chinh timestamp batch,
khong duoc dien giai thanh synchronized full-house load:

```text
H(t)      = sum x_p(t)
Htilde(t) = sum xtilde_p(t)
E_H(t)    = H(t) - Htilde(t)
```

House budget:

```text
Delta_H = epsilon_ratio * mean_warmup_event_aligned_house_load
```

Neu tong threshold active khong vuot `Delta_H` va moi event chi suppress khi
`abs(r) <= delta`, thi `abs(E_H(t)) <= Delta_H` tren batch event-aligned.

### Residual score

State giu rolling second moment `v_p`; score la:

```text
s_p = max(sqrt(max(v_p, 0)), sigma_floor_H)
```

- `exact`: moi event cap nhat bang residual that binh phuong.
- `uniform_proxy`: transmitted event dung `r^2`; suppressed event chi dung
  `delta_used^2 / 3`, khong doc residual that tren nhanh suppress.
- Cold start fallback: median household, median house, sigma floor, roi `1.0`.
- Sigma floor duoc fit tren warm-up tu percentile cua per-plug sigma duong.

Chi tiet cong thuc authoritative nam trong `docs/MATH_SPEC.md`.

## 5. Sau allocator

### `full_tx`

Threshold cua moi plug bang 0, nen valid predicted event chi suppress tai residual
bang dung 0. Day la baseline reconstruction error bang 0.

### `uniform`

Chia deu `Delta_H` cho tat ca active plug.

### `flat_variance`

Chia theo ty le score `s_p`. Neu tong score bang 0 thi fallback uniform.

### `legacy_two_stage`

Chia budget cho household theo tong score, roi chia tiep cho plug theo score.
Dai so rut gon thanh dung `flat_variance`; equivalence nay la invariant duoc test
bang unit/property/integration va CLI. Khong duoc goi mode nay la true hierarchy.

### `true_hierarchical`

Household risk ket hop mean, max va log-count cua plug score bang ba lambda co
tong bang 1. `household_fairness_alpha` tron equal household share voi risk
share; `plug_fairness_beta` tron equal plug share voi score share. Cau hinh bi
reject neu lam hierarchy suy bien ve flat theo cach dac ta cam.

### `true_hierarchical_cap`

Bat dau tu true hierarchy, sau do gioi han moi plug bang warm-up residual quantile
va house-budget fraction. Phan budget bi cat duoc water-fill trong household,
roi spill sang household con capacity. Thuat toan co deterministic tie-break,
gioi han vong lap va theo doi unused budget, cap hits, redistribution rounds,
within-household redistribution va cross-household spill.

## 6. Semantics event-time va no-leakage

Day la phan correctness quan trong nhat cua simulator:

- Event duoc stable-sort theo `(timestamp, household_id, plug_id,
  original_row_index)`.
- Moi nhom event cung timestamp la mot batch nguyen tu.
- Tat ca decision trong batch dung predictor/threshold snapshot truoc batch.
- State update chi commit sau khi moi decision trong batch da duoc tao.
- Allocation sinh sau batch `B` co `effective_after=B`, chi ap dung cho
  `timestamp > B`.
- Initial allocation o evaluation start chi dua vao warm-up/past state.
- Active set tai `B` gom plug co valid event trong `(B-T_active, B]`.
- Plug het active window co threshold 0; event dau khi quay lai bi forced transmit
  voi reason `inactive_reactivation`.
- Missing prediction cung forced transmit.
- Warm-up, optional validation va evaluation phai disjoint, theo dung thu tu.

Thu tu forced reason cua adaptive replay la: invalid value, missing prediction,
inactive reactivation, drift, count sync, time sync, roi normal dead-band.

## 7. Predictor

### Time-slice median

`TimeSliceMedianPredictor` map timestamp UTC vao slot:

```text
(day_of_week, floor(seconds_since_midnight / slot_seconds))
```

Prediction fallback theo thu tu plug-slot median, plug-global warm-up median, roi
missing prediction. Model chi fit tu warm-up.

### Dual EWMA predictor

Adaptive predictor co frozen seasonal baseline `m_p(t)` va bias `b_p(t)`:

```text
xhat_p(t) = m_p(t) + b_p(t)
b_p <- b_p + alpha * innovation
```

Transmitted event commit cung transition tren edge va server. Suppressed event
khong cap nhat shared/server-visible bias. Oracle mode co the cap nhat edge bang
suppressed actual de lam doi chung, vi vay no bi danh dau non-deployable.

Drift scale dung robust MAD voi fallback plug -> household -> house -> floor ->
`1.0`. Detector ho tro `none`, consecutive z-score va two-sided CUSUM. Drift
trigger, count sync va time sync deu la forced transmission va duoc tinh vao TR.

## 8. Data pipeline

Input chuan ho tro CSV va Parquet voi cac cot:

```text
id (optional), timestamp, value, property,
plug_id, household_id, house_id
```

Loader loc selected house, `property == load_property_value` (mac dinh 1),
timestamp range, finite/non-negative value; dong thoi ghi filter counts. Neu
thieu `id`, `original_row_index` dam bao tie-break on dinh. Numeric timestamp ho
tro seconds, milliseconds va microseconds; textual timestamp phai parse duoc UTC.

Co hai execution path:

- **Eager**: nap events vao memory, phu hop synthetic, test va sensitivity
  workflow nho.
- **Streaming/bounded-memory**: filter va external stable-sort mot lan sang keyed
  Parquet staging; cache duoc reuse neu input hash va data settings khong doi.
  Evaluation doc theo chunk nhung carry equal-timestamp batch qua bien chunk.

Streaming replay khong giu raw evaluation rows; no giu rolling state, compact
numeric arrays, aggregate theo plug/household/day, top-K heap va threshold
diagnostics. `experiment-house` van la eager workflow phong phu, nen chi dung cho
window vua memory.

## 9. Cau truc source hien tai

```text
src/hiereb/
|-- cli.py                    # Typer entrypoint va lenh inspect
|-- config.py                 # strict Pydantic YAML schema
|-- determinism.py            # chay lai va so checksum artifacts
|-- experiment.py             # orchestration cac run/comparison/validation
|-- house_experiment.py       # full Direction 1 research workflow
|-- domain/models.py          # Event, PlugKey, SplitEvents
|-- data/
|   |-- loader.py             # scan/filter/normalize CSV-Parquet
|   |-- splits.py             # time split va warm-up house load
|   |-- streaming.py          # staging, aggregate warm-up, batch iterator
|   |-- synthetic.py          # fixture Direction 1
|   `-- synthetic_direction3.py
|-- predictor/
|   |-- base.py               # Predictor Protocol
|   |-- slot_median.py        # frozen seasonal median
|   |-- adaptive.py           # edge/server EWMA state
|   `-- drift.py              # scale, z-score, CUSUM
|-- residual/state.py         # exact/proxy rolling score state
|-- allocator/
|   |-- base.py               # shared request/result + dispatch
|   |-- baselines.py          # full/uniform/flat/legacy
|   |-- hierarchical.py       # true hierarchy, caps, redistribution
|   `-- redistribution.py     # deterministic water-fill
|-- simulation/
|   |-- active_set.py
|   |-- threshold_timeline.py
|   |-- replay.py             # eager frozen replay
|   |-- streaming_replay.py   # bounded-memory frozen replay
|   `-- adaptive_replay.py    # Direction 3 dual-state replay
|-- evaluation/
|   |-- metrics.py
|   |-- diagnostics.py
|   |-- comparisons.py
|   `-- artifacts.py
`-- utils/progress.py
```

Dependency flow chinh:

```text
YAML -> AppConfig -> loader/staging -> warm-up fit
     -> predictor + residual state + active set
     -> timestamp-batch replay -> allocator snapshots
     -> metrics/diagnostics -> CSV/Parquet/JSON/PNG artifacts
```

## 10. Config model

Root YAML gom cac section:

- `experiment`: ten, seed, output dir, overwrite, allocator modes.
- `data`: path/format/house/property/timestamp/column mapping va streaming.
- `splits`: warm-up, optional validation, evaluation.
- `predictor`: time-slice median, mode list va adaptive/drift/resync settings.
- `budget`: `epsilon_ratio`.
- `replay`: allocation period, active window, tolerance, top-K.
- `residual`: estimator, rolling window, min samples, floor percentile.
- `true_hierarchy`: fairness alpha/beta va household-score lambdas.
- `cap`: quantile, multiplier, house fraction, sample/iteration/tolerance.
- `artifacts`: trace, metrics, plot va adaptive predictor trace policy.

Tat ca model deu frozen, cam unknown field va validate range. `data.path` co the
override bang `--data-path` hoac bien moi truong `HIEREB_DATA_PATH`.

Cau hinh tham khao:

- `configs/house_0.example.yaml`: synthetic Direction 1, sau allocator.
- `configs/debs_house0_{smoke,validation,evaluation}.yaml`: DEBS Direction 1.
- `configs/direction3_synthetic.yaml`: drift fixture.
- `configs/direction3_house0_{smoke,validation,evaluation}.yaml`: DEBS Direction 3.
- `configs/all_modes.example.yaml`: Cartesian matrix allocator x predictor.

## 11. CLI

Entry point duoc khai bao la `hiereb = hiereb.cli:app`:

| Lenh | Vai tro |
|---|---|
| `validate-config` | Parse/validate YAML, khong doc dataset |
| `generate-synthetic` | Tao fixture 12 gio Direction 1 |
| `generate-direction3-synthetic` | Tao fixture drift deterministic |
| `run` | Chay mot `--mode` hoac tat ca configured allocator |
| `compare` | Chay allocator modes va ghi comparison |
| `verify-legacy-equivalence` | So threshold/decision cua flat va legacy |
| `verify-determinism` | Chay hai lan, so checksum artifact phi-runtime |
| `experiment-house` | Full one-house Direction 1 workflow |
| `run-predictor` | Chay mot cap allocator/predictor Direction 3 |
| `compare-predictors` | So sanh 5 predictor hoac config matrix |
| `validate-predictor` | Staged tuning chi tren validation interval |
| `inspect-outliers` | Doc top outlier artifact |
| `inspect-drift` | Doc drift event artifact |
| `inspect-resynchronization` | Doc resynchronization event artifact |

Direction 3 command yeu cau `data.streaming.enabled=true`; comparison con yeu
cau `predictor.adaptive.enabled=true`. `--matched-tr` sweep epsilon de so sanh
predictor tai transmission ratio gan nhau.

## 12. Metrics va artifacts

Core metrics gom transmission ratio/reduction, MAE, RMSE, P95, P99, Max,
CVaR95 va CVaR99. Safety/diagnostic gom bound violations/utilization, budget
utilization, unused budget, cap hits, redistribution, forced-transmit reasons,
household budget, plug threshold, suppression streak va top-K outliers.

Moi allocator run co the sinh:

```text
resolved_config.yaml
run_metadata.json
summary.json / summary.csv
daily_metrics.csv
household_metrics.csv
plug_metrics.parquet
threshold_trace.parquet
top_outliers.parquet
filter_counts.json
plots/
```

Adaptive run bo sung `predictor_summary.json`, predictor/bias traces, drift
events, resynchronization events, recovery episodes va root-cause/trade-off
artifacts. Comparison cap cao sinh `comparison.csv`, predictor matrix CSV/JSON,
Pareto tables/plots, exact-proxy va hierarchy-effect diagnostics tuy workflow.

Artifact duoc sort truoc khi ghi; metadata co resolved config, runtime version,
config/input hash khi kha thi. `overwrite=false` fail fast neu output da ton tai.

## 13. Test suite va invariant duoc bao ve

Test duoc chia theo chu de:

- `test_config.py`: strict config, ranges, split, adaptive config.
- `test_data_predictor.py`: loader, stable sort, filtering, median fallback,
  no-leakage.
- `test_residual_allocators.py`: residual exact/proxy, floor/cold start, sau
  allocator, cap/water-fill va Hypothesis properties.
- `test_replay_integration.py`: strict boundary, atomic batch, timing,
  reactivation, bound va artifacts.
- `test_streaming.py`: staging/cache/chunk batching va eager-streaming behavior.
- `test_direction3.py`: dual state, drift, resync, deployability va adaptive
  artifacts.
- `test_house_experiment.py`: end-to-end research workflow.
- `test_cli.py`: CLI smoke/failure paths.

Invariant quan trong: `legacy_two_stage == flat_variance`; full-tx error bang 0;
threshold/cap khong am; used budget khong vuot budget; used + unused bao toan
budget trong tolerance; cap khong bi vuot; deterministic tie; equal timestamp
atomic; threshold khong effective som; deployable edge/server divergence bang 0.

## 14. Cach chay chuan

Quality gates bat buoc:

```bash
uv sync
uv run ruff check .
uv run mypy src
uv run pytest
```

Synthetic Direction 1:

```bash
uv run hiereb validate-config --config configs/house_0.example.yaml
uv run hiereb generate-synthetic --output data/synthetic_house_0.csv
uv run hiereb compare --config configs/house_0.example.yaml
uv run hiereb verify-legacy-equivalence --config configs/house_0.example.yaml
```

Synthetic Direction 3:

```bash
uv run hiereb generate-direction3-synthetic --output data/synthetic_direction3.csv
uv run hiereb compare-predictors \
  --config configs/direction3_synthetic.yaml \
  --allocator uniform \
  --matched-tr
```

DEBS data co the duoc truyen ma khong sua YAML:

```bash
export HIEREB_DATA_PATH=/absolute/path/to/house-0.csv
uv run hiereb experiment-house --config configs/debs_house0_smoke.yaml
```

## 15. Failure policy va reproducibility

He thong fail fast khi config sai, thieu cot/input, selected house rong, warm-up
rong, split overlap/unordered, tham so ngoai range, hierarchy suy bien, cap mode
khong enabled hoac output ton tai khi khong cho overwrite.

Tinh deterministic dua tren stable ordering, explicit tie-break, batch commit,
artifact sorting, seed co dinh va checksum verification. Staging cache key phu
thuoc input hash va data settings lien quan. Runtime duration/memory fields bi
loai khoi checksum comparison vi chung khong deterministic.

## 16. Trang thai repo va cac luu y khi tiep quan

- Repo hien co nhieu thay doi/chua track trong working tree; khong duoc tu y
  revert vi co the la cong viec dang do cua tac gia.
- README va source hien tai da co Direction 3, trong khi mot so tai lieu van chi
  mo ta "Direction 1".
- `docs/explain-structure.md` la ban thiet ke/phac thao cu: no neu cac file nhu
  `empirical_cdf.py`, `pareto.py`, `median_ewma.py` khong ton tai theo layout
  hien tai. Khong dung file nay lam module map authoritative.
- `docs/ARCHITECTURE.md` cung con cay thu muc cu o mot so dong (vi du residual
  tach file), du semantics tong quat van huu ich.
- `docs/ACCEPTANCE_CRITERIA.md` yeu cau `IMPLEMENTATION_REPORT.md`, nhung file do
  khong co trong working tree tai thoi diem lap tai lieu nay. Day la gap can xu ly
  neu tuyen bo toan bo acceptance criteria da pass.
- `outputs/` chua sample/generated research results; chung la evidence va dau ra,
  khong phai implementation source.
- `visualize_result.py`, notebook va `scripts/consolidate_results.py` la cong cu
  phan tich hau ky nam ngoai package core.

## 17. Thu tu doc de onboard nhanh

1. Doc file nay de nam ban do tong the.
2. Doc `docs/MATH_SPEC.md` de hieu cong thuc va invariant authoritative.
3. Doc `src/hiereb/config.py` va mot YAML gan use case can chay.
4. Doc `simulation/replay.py` hoac `simulation/adaptive_replay.py` de hieu order.
5. Doc `allocator/base.py`, `baselines.py`, `hierarchical.py` de hieu allocation.
6. Doc test tuong ung truoc khi sua behavior.
7. Chay day du quality gates va CLI acceptance commands truoc khi ket luan.

## 18. Nguon tham chieu trong repo

- `AGENTS.md`: mission, stack, scope, rules va commands bat buoc.
- `README.md`: huong dan van hanh cap cao va examples.
- `docs/PRD.md`: yeu cau Direction 1.
- `docs/MATH_SPEC.md`: cong thuc va semantics authoritative.
- `docs/ARCHITECTURE.md`: architecture intent.
- `docs/EXPERIMENT_AND_TESTS.md`: ke hoach thi nghiem/test.
- `docs/ACCEPTANCE_CRITERIA.md`: dieu kien hoan thanh.
- `IMPLEMENTATION_PLAN.md`: ke hoach trien khai da lap.
- `REVIEW_REPORT.md`: ket qua review gan day, can doi chieu voi source hien tai.

