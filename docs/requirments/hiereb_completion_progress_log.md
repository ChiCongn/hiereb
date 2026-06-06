# HierEB - Progress log hoàn thành requirements

Ngày tạo: 2026-06-05
File kế hoạch: `docs/requirments/hiereb_completion_prompt_plan.md`
Nguồn audit: `docs/requirments/hiereb_codebase_audit_report.md`
Checklist đối chiếu: `docs/requirments/hiereb_verification_checklist.md`

Mục tiêu của file này là theo dõi tiến độ biến các vấn đề trong audit thành
thay đổi có thể kiểm chứng. Mỗi phase chỉ được đánh dấu `done` khi có đủ bằng
chứng: source đã sửa, test đã chạy, kết quả đã ghi lại.

## 1. Quy ước trạng thái

| Trạng thái | Ý nghĩa |
|---|---|
| `todo` | Chưa bắt đầu |
| `in_progress` | Đang thực hiện |
| `blocked` | Bị chặn, cần quyết định hoặc dữ liệu bổ sung |
| `done` | Đã hoàn thành và có bằng chứng kiểm chứng |
| `skipped` | Bỏ qua có lý do rõ ràng |

Quy tắc cập nhật:

1. Không đánh dấu `done` nếu chưa chạy test hoặc chưa ghi lý do không thể chạy.
2. Mỗi phase phải có ít nhất một dòng log ở mục `6. Nhật ký cập nhật`.
3. Nếu test fail, giữ trạng thái `in_progress` hoặc `blocked`, không sửa kết quả
   cho đẹp.
4. Khi có thay đổi source, ghi rõ file đã sửa và lệnh test đã chạy.
5. Không tick checklist nếu chưa có evidence tương ứng.

## 2. Tổng quan tiến độ

| Phase | Trạng thái | Prompt | Mục tiêu | Bằng chứng hiện tại |
|---|---|---|---|---|
| P00 | `done` | Prompt 00 | Baseline snapshot và test hiện tại | `2026-06-05T20:34:03+07:00`; pytest `111 passed in 0.84s` |
| P01 | `done` | Prompt 01 | Data contract: `property=1`, window, invalid, duplicate, sort | `2026-06-05T20:44:48+07:00`; related tests `50 passed`; full pytest `118 passed` |
| P02 | `done` | Prompt 02 | Predictor và missing prediction forced transmit | `2026-06-05T21:11:37+07:00`; related tests `42 passed`; full pytest `121 passed` |
| P03 | `done` | Prompt 03 | Event decisions và reconstruction metric | `2026-06-05T21:19:59+07:00`; aggregator/simulator tests `17 passed`; full pytest `124 passed` |
| P04A | `done` | Prompt 04 | Uniform active-budget baseline | `2026-06-05T21:31:39+07:00`; related tests `32 passed`; full pytest `132 passed` |
| P04B | `done` | Prompt 05 | HierEB allocator budget/timing/sigma_floor | `2026-06-05T21:52:45+07:00`; allocator/ML tests `30 passed`; full pytest `143 passed` |
| P05 | `done` | Prompt 06 | Sáu CSV output đúng schema | `2026-06-06T08:10:49+07:00`; export/aggregator tests `15 passed`; full pytest `147 passed` |
| P06 | `done` | Prompt 07 | Rolling metrics và summary metrics | `2026-06-06T08:15:16+07:00`; metrics/export tests `18 passed`; full pytest `150 passed` |
| P07 | `todo` | Prompt 08 | Sweep runner và metadata `run_id` | Chưa có |
| P08 | `todo` | Prompt 09 | Pareto SVG và dashboard update | Chưa có |
| P09 | `todo` | Prompt 10 | Final docs cleanup và verification run | Chưa có |
| P10 | `todo` | Prompt 10 | Tick verification checklist cuối cùng | Chưa có |

Trạng thái tài liệu điều phối:

| ID | Trạng thái | Nội dung | Bằng chứng |
|---|---|---|---|
| DOC-PLAN | `done` | Tạo thứ tự triển khai và prompt chi tiết | `docs/requirments/hiereb_completion_prompt_plan.md` |
| DOC-LOG | `done` | Tạo progress log theo dõi tiến trình | File hiện tại |

## 3. Bảng công việc chi tiết

| ID | Ưu tiên | Trạng thái | Phase | Acceptance evidence cần có | Lệnh kiểm chứng chính | Ghi chú |
|---|---|---|---|---|---|---|
| P0-01 | P0 | `done` | P01 | Default experiment dùng `property=1`; docs không còn nói `property=0` là load | `rg -n "property.*0.*load\|PROPERTY_FILTER=0\|--property 0" config .env.example README.md scripts src tests` | Dùng cho DEBS load |
| P0-02 | P0 | `done` | P01 | Warm-up/evaluation window tuyệt đối, không overlap | `pytest tests/test_loader.py -q` | Cần deterministic |
| P0-03 | P0 | `done` | P02 | Missing prediction luôn forced transmit | `pytest tests/test_predictor.py tests/test_plug_state.py -q` | Không dùng last value làm fallback metric |
| P0-04 | P0 | `done` | P03 | Metric chính là `actual_load` vs `reconstructed_load` | `pytest tests/test_aggregator.py tests/test_simulator_main.py -q` | Không dùng residual predictor làm house RMSE |
| P0-05 | P0 | `done` | P03 | `full_tx` có reconstruction error bằng 0 | `pytest tests/test_aggregator.py -q` | Predictor error có thể ghi riêng |
| P0-06 | P0 | `done` | P04A | Uniform dùng active-budget: `delta_p = Delta_H / N_budget_active` | `pytest tests/test_plug_state.py tests/test_simulator_main.py -q` | Có metadata active plug trong `HouseState` |
| P0-07 | P0 | `done` | P04B | HierEB allocator bảo toàn budget, không inflate do `delta_min` | `pytest tests/test_allocator.py -q` | Có assertion tổng threshold |
| P1-01 | P1 | `done` | P01 | Invalid value, duplicate và sort stable được xử lý | `pytest tests/test_loader.py tests/test_partition_debs_by_house.py -q` | Duplicate giữ `id` lớn nhất |
| P1-02 | P1 | `done` | P04B | `sigma_floor` cố định từ warm-up toàn house | `pytest tests/test_allocator.py -q` | Không dùng percentile runtime |
| P1-03 | P1 | `done` | P04B | Threshold update có `threshold_version` và `effective_after_time` | `pytest tests/test_allocator.py tests/test_simulator_main.py -q` | Tránh retroactive decision |
| P1-04 | P1 | `todo` | P04A/P04B | Inactive plug và reactivation không phá budget | `pytest tests/test_allocator.py tests/test_plug_state.py -q` | Active set cần rõ |
| P1-05 | P1 | `done` | P06 | Rolling metrics tính theo event-time window | `pytest tests/test_csv_exporter.py tests/test_aggregator.py -q` | Cửa sổ `(t - 3600, t]`, min 60 timestamp |
| P1-06 | P1 | `done` | P05 | Export đủ 6 CSV bắt buộc: experiment_runs, event_decisions, house_timeseries, plug_metrics, house_summary, threshold_trace | `pytest tests/test_csv_exporter.py -q` | Schema ổn định bằng `CSV_SCHEMAS` |
| P2-01 | P2 | `todo` | P07 | Sweep runner có config grid và metadata `run_id` | `pytest tests/test_experiment_runner.py -q` | Dùng để so sánh Pareto |
| P2-02 | P2 | `todo` | P08 | Có SVG Pareto/transmission/RMSE từ output CSV | `pytest tests/test_dashboard.py -q` | Nếu không có test UI, ghi smoke evidence |
| P2-03 | P2 | `todo` | P08 | Dashboard hiển thị `P95`, `Delta_H`, `sigma_floor`, `active_plug_count` | `pytest tests/test_dashboard.py -q` | Post-hoc là đủ |
| P2-04 | P2 | `done` | P06 | Plug metrics có `event_count`, `transmitted_count`, `suppressed_count`, `missing_prediction_count` | `pytest tests/test_csv_exporter.py -q` | Đã test tách reconstruction/prediction metrics |

## 4. Baseline

Baseline phải được ghi sau khi chạy Prompt 00.

| Trường | Giá trị |
|---|---|
| Thời điểm chạy | `2026-06-05T20:34:03+07:00` |
| Git status trước khi sửa | Worktree đã dirty trước khi implement; xem snapshot trong log `2026-06-05T20:34:03+07:00 - Phase P00` |
| Lệnh test baseline | `.venv/bin/python -m pytest -q` |
| Kết quả baseline | `111 passed in 0.84s` |
| Ghi chú | Baseline đã được xác nhận lại. Chưa sửa source code trong phase này; chỉ cập nhật progress log. |

## 5. Evidence theo phase

### P00 - Baseline snapshot

- Trạng thái: `done`
- File đã sửa: `docs/requirments/hiereb_completion_progress_log.md`
- Test đã chạy: `.venv/bin/python -m pytest -q`
- Kết quả: `111 passed in 0.84s`
- Blocker: không có
- Next step: chạy Prompt 01 trong `docs/requirments/hiereb_completion_prompt_plan.md`

### P01 - Data contract

- Trạng thái: `done`
- File đã sửa: `config/settings.py`, `.env.example`, `src/simulator/loader.py`,
  `src/simulator/main.py`, `src/ml_hiereb/main.py`, `src/ml_hiereb/predictor.py`,
  `scripts/partition_debs_by_house.py`, `README.md`, `tests/test_loader.py`,
  `tests/test_partition_debs_by_house.py`, `tests/test_simulator_main.py`
- Test đã chạy:
  - `.venv/bin/python -m pytest tests/test_loader.py tests/test_partition_debs_by_house.py -q`
  - `.venv/bin/python -m pytest tests/test_loader.py tests/test_partition_debs_by_house.py tests/test_ml_main.py tests/test_simulator_main.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "property.*0.*load|Property=0|PROPERTY_FILTER=0|--property 0|instantaneous load|property_filter: int = 0|Expected to contain only Property=0|Watts \(Property=0" config .env.example README.md scripts src tests`
- Kết quả:
  - Loader/partition tests: `41 passed in 0.83s`
  - Related tests: `50 passed in 0.68s`
  - Full suite: `118 passed in 0.79s`
  - `rg` verification: không còn match trong `config`, `.env.example`, `README.md`, `scripts`, `src`, `tests`
- Blocker: không có
- Next step: chạy Prompt 02 để xử lý predictor và missing prediction forced transmit

### P02 - Predictor và missing prediction

- Trạng thái: `done`
- File đã sửa: `src/ml_hiereb/predictor.py`, `src/simulator/plug_state.py`,
  `src/simulator/main.py`, `src/simulator/kafka_listeners.py`,
  `src/simulator/stats.py`, `tests/test_predictor.py`, `tests/test_plug_state.py`,
  `tests/test_simulator_main.py`, `tests/test_stats.py`
- Test đã chạy:
  - `python3 -m compileall src/ml_hiereb src/simulator tests`
  - `.venv/bin/python -m pytest tests/test_predictor.py tests/test_plug_state.py tests/test_simulator_main.py -q`
  - `.venv/bin/python -m pytest tests/test_predictor.py tests/test_plug_state.py tests/test_simulator_main.py tests/test_stats.py -q`
  - `.venv/bin/python -m pytest -q`
- Kết quả:
  - Compile: pass
  - Predictor/plug_state/simulator tests: `35 passed in 0.33s`
  - Related tests with stats: `42 passed in 0.39s`
  - Full suite: `121 passed in 0.68s`
- Blocker: không có
- Next step: chạy Prompt 03 để sửa event decisions và reconstruction metric

### P03 - Event decisions và reconstruction metric

- Trạng thái: `done`
- File đã sửa: `src/simulator/main.py`, `src/aggregator/main.py`,
  `src/aggregator/db_writer.py`, `scripts/init_db.sql`,
  `tests/test_aggregator.py`, `tests/test_simulator_main.py`
- Test đã chạy:
  - `python3 -m compileall src/aggregator src/simulator tests`
  - `.venv/bin/python -m pytest tests/test_aggregator.py tests/test_simulator_main.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "all_suppressed_e_h_is_zero|suppressed.*predicted.*actual|e_h contribution.*0|can't know actual|predicted-for-suppressed|actual estimate|suppressed plugs use predicted" src tests scripts`
- Kết quả:
  - Compile: pass
  - Aggregator/simulator tests: `17 passed in 0.31s`
  - Full suite: `124 passed in 0.75s`
  - `rg` verification: không còn match cho logic cũ `all suppressed e_h = 0` hoặc suppressed dùng prediction làm actual
- Blocker: không có
- Next step: chạy Prompt 04 để sửa uniform active-budget baseline

### P04A - Uniform active-budget baseline

- Trạng thái: `done`
- File đã sửa: `config/settings.py`, `.env.example`,
  `src/simulator/plug_state.py`, `src/simulator/main.py`,
  `tests/test_plug_state.py`, `tests/test_simulator_main.py`
- Test đã chạy:
  - `python3 -m compileall config src/simulator tests`
  - `.venv/bin/python -m pytest tests/test_plug_state.py tests/test_simulator_main.py tests/test_ml_main.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "settings\\.UNIFORM_DELTA|fixed UNIFORM_DELTA|uniform = fixed|fixed threshold suppression|for 'uniform' mode" config .env.example README.md src tests`
- Kết quả:
  - Compile: pass.
  - Plug state/simulator/ML helper tests: `32 passed in 0.32s`.
  - Full suite: `132 passed in 0.75s`.
  - `rg` verification: không có output, tức baseline uniform không còn đọc `settings.UNIFORM_DELTA` hoặc mô tả là fixed threshold.
- Acceptance evidence:
  - Uniform baseline dùng `delta_p = Delta_H / N_budget_active_plugs_H(t)`.
  - `ACTIVE_WINDOW_SECONDS=3600` được thêm vào settings và `.env.example`.
  - Plug ngoài active window nhận `delta_p = 0` ở allocation kế tiếp.
  - Plug inactive reappear forced transmit với `plug_status=reactivated`, `reason=inactive_reactivation`.
  - Missing prediction ưu tiên hơn reactivation: `reason=missing_prediction`, `predicted_load=null`, `residual=null`.
  - Uniform allocation được stage tại `allocation_time` và chỉ activate khi `timestamp > allocation_time`.
  - `UNIFORM_DELTA` chỉ còn là cấu hình legacy/debug, không dùng trong baseline uniform decision.
- Blocker: không có
- Next step: chạy Prompt 05 để sửa HierEB allocator budget/timing/sigma_floor

### P04B - HierEB allocator

- Trạng thái: `done`
- File đã sửa: `src/ml_hiereb/allocator.py`, `src/ml_hiereb/main.py`,
  `src/simulator/kafka_listeners.py`, `src/simulator/plug_state.py`,
  `src/simulator/main.py`, `tests/test_allocator.py`, `tests/test_ml_main.py`,
  `tests/test_simulator_main.py`
- Test đã chạy:
  - `python3 -m compileall src/ml_hiereb src/simulator tests`
  - `.venv/bin/python -m pytest tests/test_allocator.py tests/test_ml_main.py tests/test_simulator_main.py tests/test_plug_state.py -q`
  - `.venv/bin/python -m pytest tests/test_allocator.py tests/test_ml_main.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "max\\([^\\n]*0\\.1|floor at 0\\.1|minimum floor 0\\.1|minimum 0\\.1|delta_min|censored_deltas" src/ml_hiereb src/simulator`
- Kết quả:
  - Compile: pass.
  - Allocator/ML/simulator/plug-state tests: `59 passed in 0.32s`.
  - Allocator/ML acceptance tests: `30 passed in 0.34s`.
  - Full suite: `143 passed in 0.79s`.
  - `rg` verification: không có output trong source cho floor `0.1`, `delta_min`, hoặc buffer censored cũ.
- Acceptance evidence:
  - `sigma_floor_H` được tính một lần từ warm-up residual bằng percentile 5 của sigma dương; fallback `1.0 W`.
  - Allocator giữ `sigma_floor_H` cố định trong evaluation.
  - Weight dùng `w_p = max(sqrt(max(v_p, 0)), sigma_floor_H)` cho plug đủ mẫu.
  - Cold start dùng median household weight, rồi median house weight, rồi fallback `1.0 W`.
  - Cập nhật transmitted dùng residual thật; suppressed dùng `delta_used^2 / 3` với threshold cũ tại decision.
  - Rolling window giữ `1000` effective residual samples per plug.
  - Inactive plug nhận `delta=0` và không chiếm budget dương; runtime ML loop tính active set từ variance event-time.
  - Có `ThresholdTrace` nội bộ với `allocation_time`, `effective_after_time`, `threshold_version`.
  - Threshold message Kafka có metadata trace; simulator stage threshold và chỉ activate khi `timestamp > effective_after_time`.
  - Test budget-bound bắt `sum(delta_p) <= Delta_H` cả trường hợp epsilon nhỏ/nhiều plug.
- Blocker: không có
- Next step: chạy Prompt 06 để tạo sáu CSV output đúng schema

### P05 - Sáu CSV output

- Trạng thái: `done`
- File đã sửa: `src/aggregator/csv_exporter.py`,
  `scripts/export_required_csv.py`, `tests/test_csv_exporter.py`
- Test đã chạy:
  - `python3 -m compileall src/aggregator scripts/export_required_csv.py tests/test_csv_exporter.py`
  - `.venv/bin/python -m pytest tests/test_csv_exporter.py -q`
  - `.venv/bin/python -m pytest tests/test_csv_exporter.py tests/test_aggregator.py -q`
  - `.venv/bin/python -m pytest -q`
- Kết quả:
  - Compile: pass.
  - CSV exporter schema tests: `4 passed in 0.04s`.
  - Exporter/aggregator tests: `15 passed in 0.17s`.
  - Full suite: `147 passed in 0.83s`.
- Acceptance evidence:
  - Có script `scripts/export_required_csv.py` tạo đủ 6 CSV từ JSON artifact nhỏ.
  - Có module `src/aggregator/csv_exporter.py` với schema cố định cho:
    `experiment_runs.csv`, `event_decisions.csv`, `house_timeseries.csv`,
    `plug_metrics.csv`, `house_summary.csv`, `threshold_trace.csv`.
  - Tất cả CSV có cột `mode` trực tiếp.
  - Test schema kiểm tra header exact columns cho cả 6 CSV.
  - Null được ghi thành field rỗng trong CSV, không ghi `0` giả.
  - `event_decisions.csv` có `plug_status`, `is_forced_transmit`, `reason`.
  - `house_timeseries.csv` có `rolling_tr_1h`, `rolling_rmse_1h`,
    `rolling_insufficient_data`.
  - `plug_metrics.csv` có reconstruction/prediction RMSE/MAE,
    `inactive_reactivation_count`, `missing_prediction_count`.
  - `house_summary.csv` có `p95_abs_house_error`, `max_abs_house_error`,
    `Delta_H`, `epsilon_ratio`.
  - `threshold_trace.csv` có `allocation_time`, `effective_after_time`,
    `threshold_version`, `trace_granularity`, `sigma_floor_used`,
    `delta_used_for_censored_update`.
  - `experiment_runs.csv` có `uniform_delta_initial`, `sigma_floor_used`,
    sweep metadata.
- Blocker: không có
- Next step: chạy Prompt 07 để mở rộng rolling/summary metrics runtime nếu cần

### P06 - Rolling và summary metrics

- Trạng thái: `done`
- File đã sửa: `src/aggregator/csv_exporter.py`, `tests/test_csv_exporter.py`
- Test đã chạy:
  - `python3 -m compileall src/aggregator tests/test_csv_exporter.py`
  - `.venv/bin/python -m pytest tests/test_csv_exporter.py tests/test_aggregator.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "rolling_insufficient_data|ROLLING_MIN_VALID_TIMESTAMPS|ROLLING_WINDOW_SECONDS|rmse_house_error|p95_abs_house_error|full_tx" src/aggregator tests/test_csv_exporter.py tests/test_aggregator.py`
- Kết quả:
  - Compile: pass.
  - Metrics/export tests: `18 passed in 0.17s`.
  - Full suite: `150 passed in 0.78s`.
  - `rg` verification: thấy constants/tests liên quan rolling, P95/RMSE và full_tx.
- Acceptance evidence:
  - Rolling window dùng event-time seconds với cửa sổ `(t - 3600, t]`; event đúng biên `t - 3600` bị loại.
  - `rolling_insufficient_data = true` khi cửa sổ có dưới `60` timestamp hợp lệ.
  - Test fixture timestamp không đều chứng minh rolling RMSE tính theo event-time, không theo số row cố định hoặc wall-clock.
  - `house_summary.csv` tính `MAE_H`, `RMSE_H`, `P95_H`, `MAX_H` trên `E_H(t)`.
  - `plug_metrics.csv` tách `rmse_reconstruction`/`mae_reconstruction` và `rmse_prediction`/`mae_prediction`.
  - `full_tx` reconstruction RMSE/MAE bằng `0`, trong khi prediction RMSE/MAE vẫn được ghi riêng.
- Blocker: không có
- Next step: chạy Prompt 08 để thêm sweep runner và metadata `run_id`

### P07 - Sweep runner

- Trạng thái: `todo`
- File dự kiến: runner/config/script hiện có, `tests/test_experiment_runner.py`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P06 done

### P08 - Pareto SVG và dashboard

- Trạng thái: `todo`
- File dự kiến: dashboard/export chart files hiện có, `tests/test_dashboard.py`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P07 có dữ liệu sweep

### P09 - Final verification

- Trạng thái: `todo`
- File dự kiến: `README.md`, docs liên quan, checklist
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P08 done

### P10 - Checklist final

- Trạng thái: `todo`
- File dự kiến: `docs/requirments/hiereb_verification_checklist.md`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P09 verification pass

## 6. Nhật ký cập nhật

### 2026-06-05 - Khởi tạo tài liệu điều phối

- Trạng thái: `done`
- Đã tạo:
  - `docs/requirments/hiereb_completion_prompt_plan.md`
  - `docs/requirments/hiereb_completion_progress_log.md`
- Test/source verification:
  - Chưa chạy test source trong bước lập kế hoạch này.
  - Cần chạy Prompt 00 trước khi sửa code.
- Ghi chú:
  - Audit trước đó ghi nhận `.venv/bin/python -m pytest -q` đạt
    `111 passed in 0.81s`, nhưng kết quả này cần được xác nhận lại ở baseline.
- Next step:
  - Chạy Prompt 00 để ghi baseline hiện tại.

### 2026-06-05T20:34:03+07:00 - Phase P00: Baseline snapshot

- Trạng thái: `done`
- File đã sửa:
  - `docs/requirments/hiereb_completion_progress_log.md`
- Lệnh đã chạy:
  - `git status --short`
  - `.venv/bin/python -m pytest -q`
- Kết quả:
  - `git status --short`: worktree đã có nhiều thay đổi trước khi implement.
  - `.venv/bin/python -m pytest -q`: `111 passed in 0.84s`.
- Git status snapshot:

```text
 M .env.example
 M README.md
 M config/settings.py
 M docs/demo_system_guide.md
 M grafana/dashboards/hiereb_main.json
 M scripts/inspect_simulator.sh
 M scripts/verify_e2e.sh
 M src/ml_hiereb/main.py
 M src/simulator/loader.py
 M src/simulator/main.py
 M tests/test_loader.py
 M tests/test_ml_main.py
?? docs/TAI_LIEU_HE_THONG.md
?? docs/TOM_TAT_BAO_CAO.md
?? docs/TONG_HOP_DU_AN_HIEREB.md
?? docs/academic_report.tex
?? docs/assets/
?? docs/bao-cao-bo-cuc.md
?? docs/bao_cao_bao_cuc_chi_tiet.md
?? docs/bao_cao_bo_cuc_template.tex
?? docs/danh_sach_hinh_anh_bieu_do_code.md
?? docs/danh_sach_hinh_anh_chart_main_tex.md
?? docs/list-of-figures.txt
?? docs/main.tex
?? docs/minh-hoa.md
?? docs/mo-ta-y-tuong.txt
?? docs/references.bib
?? docs/requirments/
?? docs/system_documentation.md
?? docs/system_specification.tex
?? docs/tong_hop_cau_hinh_hiereb.md
?? feedback.md
?? scripts/partition_debs_by_house.py
?? tests/test_partition_debs_by_house.py
```

- Acceptance evidence:
  - Đã đọc các file audit/requirements/checklist/progress log được yêu cầu.
  - Baseline test chạy thành công.
  - Không sửa source code trong phase này.
- Blocker:
  - Không có.
- Next step:
  - Chạy Prompt 01 để implement data contract.

### 2026-06-05T20:44:48+07:00 - Phase P01: Data contract

- Trạng thái: `done`
- File đã sửa:
  - `config/settings.py`
  - `.env.example`
  - `src/simulator/loader.py`
  - `src/simulator/main.py`
  - `src/ml_hiereb/main.py`
  - `src/ml_hiereb/predictor.py`
  - `scripts/partition_debs_by_house.py`
  - `README.md`
  - `tests/test_loader.py`
  - `tests/test_partition_debs_by_house.py`
  - `tests/test_simulator_main.py`
  - `docs/requirments/hiereb_completion_progress_log.md`
- Lệnh đã chạy:
  - `python3 -m compileall config src scripts tests`
  - `.venv/bin/python -m pytest tests/test_loader.py tests/test_partition_debs_by_house.py -q`
  - `.venv/bin/python -m pytest tests/test_loader.py tests/test_partition_debs_by_house.py tests/test_ml_main.py tests/test_simulator_main.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "property.*0.*load|Property=0|PROPERTY_FILTER=0|--property 0|instantaneous load|property_filter: int = 0|Expected to contain only Property=0|Watts \(Property=0" config .env.example README.md scripts src tests`
- Kết quả:
  - Compile: pass.
  - Loader/partition tests: `41 passed in 0.83s`.
  - Related tests: `50 passed in 0.68s`.
  - Full suite: `118 passed in 0.79s`.
  - `rg` verification: không có output, tức không còn default/comment `property=0` là load trong các path kiểm tra.
- Acceptance evidence:
  - Default `PROPERTY_FILTER=1` trong settings và `.env.example`.
  - Thêm `WARMUP_START`, `WARMUP_END`, `EVAL_START`, `EVAL_END` theo Unix UTC cố định.
  - Loader lọc `property=1`, `house_id`, time window, `value >= 0`, finite value.
  - Loader giữ `id` và `property` trong DataFrame/model để audit.
  - Duplicate key `(timestamp, house_id, household_id, plug_id, property)` giữ dòng có `id` lớn nhất.
  - Sort ổn định theo `timestamp, house_id, household_id, plug_id, property, id`.
  - Có `DebsLoadStats` để trả count invalid/duplicate khi chưa có metadata writer.
- Blocker:
  - Không có.
- Next step:
  - Chạy Prompt 02 để sửa missing prediction forced transmit.

### 2026-06-05T21:11:37+07:00 - Phase P02: Predictor và missing prediction

- Trạng thái: `done`
- File đã sửa:
  - `src/ml_hiereb/predictor.py`
  - `src/simulator/plug_state.py`
  - `src/simulator/main.py`
  - `src/simulator/kafka_listeners.py`
  - `src/simulator/stats.py`
  - `tests/test_predictor.py`
  - `tests/test_plug_state.py`
  - `tests/test_simulator_main.py`
  - `tests/test_stats.py`
  - `docs/requirments/hiereb_completion_progress_log.md`
- Lệnh đã chạy:
  - `python3 -m compileall src/ml_hiereb src/simulator tests`
  - `.venv/bin/python -m pytest tests/test_predictor.py tests/test_plug_state.py tests/test_simulator_main.py -q`
  - `.venv/bin/python -m pytest tests/test_predictor.py tests/test_plug_state.py tests/test_simulator_main.py tests/test_stats.py -q`
  - `.venv/bin/python -m pytest -q`
- Kết quả:
  - Compile: pass.
  - Predictor/plug_state/simulator tests: `35 passed in 0.33s`.
  - Related tests with stats: `42 passed in 0.39s`.
  - Full suite: `121 passed in 0.68s`.
- Acceptance evidence:
  - Unknown plug trong predictor trả missing prediction (`{}` hoặc `None`), không trả `0.0`.
  - `PlugState.get_prediction()` chỉ trả prediction cache hoặc `None`; không fallback sang `last_value` hoặc `0.0`.
  - Missing prediction trong suppression mode forced transmit với `predicted_load=null`, `residual=null`, `reason=missing_prediction`, `is_forced_transmit=true`.
  - `last_value` vẫn được lưu để debug/state nhưng test xác nhận không được dùng làm prediction fallback.
  - Strict `abs_residual > delta` vẫn giữ: bằng delta thì suppress.
- Blocker:
  - Không có.
- Next step:
  - Chạy Prompt 03 để sửa event decisions và reconstruction metric.

### 2026-06-05T21:19:59+07:00 - Phase P03: Event decisions và reconstruction metric

- Trạng thái: `done`
- File đã sửa:
  - `src/simulator/main.py`
  - `src/aggregator/main.py`
  - `src/aggregator/db_writer.py`
  - `scripts/init_db.sql`
  - `tests/test_aggregator.py`
  - `tests/test_simulator_main.py`
  - `docs/requirments/hiereb_completion_progress_log.md`
- Lệnh đã chạy:
  - `python3 -m compileall src/aggregator src/simulator tests`
  - `.venv/bin/python -m pytest tests/test_aggregator.py tests/test_simulator_main.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "all_suppressed_e_h_is_zero|suppressed.*predicted.*actual|e_h contribution.*0|can't know actual|predicted-for-suppressed|actual estimate|suppressed plugs use predicted" src tests scripts`
- Kết quả:
  - Compile: pass.
  - Aggregator/simulator tests: `17 passed in 0.31s`.
  - Full suite: `124 passed in 0.75s`.
  - `rg` verification: không có output cho các pattern logic cũ.
- Acceptance evidence:
  - Simulator message giữ đủ `actual_load`, nullable `predicted_load`, `reconstructed_load`, `transmitted`, `decision`, `reason`, `plug_status`, `is_forced_transmit`.
  - Khi suppress: `reconstructed_load = predicted_load`.
  - Khi transmit: `reconstructed_load = actual_load`.
  - Aggregator tính `E_H(t) = sum(actual_load) - sum(reconstructed_load)` trên plug có observed event trong batch.
  - `full_tx` reconstruction error bằng `0` trong test.
  - Test `all suppressed` với actual khác prediction cho `e_h != 0`; không còn test khẳng định `all_suppressed_e_h_is_zero`.
  - DB schema/record có thêm `reconstructed_load` để lưu reconstruction series.
- Blocker:
  - Không có.
- Next step:
  - Chạy Prompt 04 để sửa uniform active-budget baseline.

### 2026-06-05T21:31:39+07:00 - Phase P04A: Uniform active-budget baseline

- Trạng thái: `done`
- File đã sửa:
  - `config/settings.py`
  - `.env.example`
  - `src/simulator/plug_state.py`
  - `src/simulator/main.py`
  - `tests/test_plug_state.py`
  - `tests/test_simulator_main.py`
  - `docs/requirments/hiereb_completion_progress_log.md`
- Lệnh đã chạy:
  - `python3 -m compileall config src/simulator tests`
  - `.venv/bin/python -m pytest tests/test_plug_state.py tests/test_simulator_main.py tests/test_ml_main.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "settings\\.UNIFORM_DELTA|fixed UNIFORM_DELTA|uniform = fixed|fixed threshold suppression|for 'uniform' mode" config .env.example README.md src tests`
- Kết quả:
  - Compile: pass.
  - Plug state/simulator/ML helper tests: `32 passed in 0.32s`.
  - Full suite: `132 passed in 0.75s`.
  - `rg` verification: không có output cho fixed `UNIFORM_DELTA` trong baseline path.
- Acceptance evidence:
  - `uniform` không còn dùng fixed `settings.UNIFORM_DELTA` trong `build_kafka_message`.
  - Uniform per-plug threshold được tính từ `Delta_H / N_budget_active_plugs_H(t)`.
  - Active window dùng event-time `3600` giây.
  - Inactive plug nhận delta `0` ở allocation kế tiếp; reappear forced transmit.
  - Missing prediction giữ ưu tiên cao hơn inactive reactivation.
  - Allocation mới chỉ activate cho event có `timestamp > allocation_time`.
- Blocker:
  - Không có.
- Next step:
  - Chạy Prompt 05 để sửa HierEB allocator budget/timing/sigma_floor.

### 2026-06-05T21:52:45+07:00 - Phase P04B: HierEB allocator

- Trạng thái: `done`
- File đã sửa:
  - `src/ml_hiereb/allocator.py`
  - `src/ml_hiereb/main.py`
  - `src/simulator/kafka_listeners.py`
  - `src/simulator/plug_state.py`
  - `src/simulator/main.py`
  - `tests/test_allocator.py`
  - `tests/test_ml_main.py`
  - `tests/test_simulator_main.py`
  - `docs/requirments/hiereb_completion_progress_log.md`
- Lệnh đã chạy:
  - `python3 -m compileall src/ml_hiereb src/simulator tests`
  - `.venv/bin/python -m pytest tests/test_allocator.py tests/test_ml_main.py tests/test_simulator_main.py tests/test_plug_state.py -q`
  - `.venv/bin/python -m pytest tests/test_allocator.py tests/test_ml_main.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "max\\([^\\n]*0\\.1|floor at 0\\.1|minimum floor 0\\.1|minimum 0\\.1|delta_min|censored_deltas" src/ml_hiereb src/simulator`
- Kết quả:
  - Compile: pass.
  - Allocator/ML/simulator/plug-state tests: `59 passed in 0.32s`.
  - Allocator/ML acceptance tests: `30 passed in 0.34s`.
  - Full suite: `143 passed in 0.79s`.
  - `rg` verification: không có output cho hard-coded floor `0.1`, `delta_min`, hoặc censored buffer cũ trong source.
- Acceptance evidence:
  - Allocator dùng fixed `sigma_floor_H` từ warm-up và không recompute percentile runtime.
  - Không còn min delta dương trong baseline; allocation có assertion `sum(delta_p) <= Delta_H`.
  - Variance bằng `0` vẫn nhận weight bằng `sigma_floor_H`.
  - Cold start dùng fallback median household, median house, rồi `1.0 W`.
  - Suppressed update dùng `delta_used^2 / 3`; transmitted update dùng residual thật.
  - Rolling window đúng `1000` effective residual samples.
  - Inactive plugs nhận delta `0` và không chiếm budget; active set được tính từ timestamp trên variance stream.
  - Threshold trace/payload có `allocation_time`, `effective_after_time`, `threshold_version`.
  - Simulator chỉ apply staged HierEB threshold khi event timestamp lớn hơn `effective_after_time`.
- Blocker:
  - Không có.
- Next step:
  - Chạy Prompt 06 để tạo sáu CSV output đúng schema.

### 2026-06-06T08:10:49+07:00 - Phase P05: Sáu CSV output

- Trạng thái: `done`
- File đã sửa:
  - `src/aggregator/csv_exporter.py`
  - `scripts/export_required_csv.py`
  - `tests/test_csv_exporter.py`
  - `docs/requirments/hiereb_completion_progress_log.md`
- Lệnh đã chạy:
  - `python3 -m compileall src/aggregator scripts/export_required_csv.py tests/test_csv_exporter.py`
  - `.venv/bin/python -m pytest tests/test_csv_exporter.py -q`
  - `.venv/bin/python -m pytest tests/test_csv_exporter.py tests/test_aggregator.py -q`
  - `.venv/bin/python -m pytest -q`
- Kết quả:
  - Compile: pass.
  - CSV exporter schema tests: `4 passed in 0.04s`.
  - Exporter/aggregator tests: `15 passed in 0.17s`.
  - Full suite: `147 passed in 0.83s`.
- Acceptance evidence:
  - `scripts/export_required_csv.py` tạo đủ 6 CSV bắt buộc từ một JSON artifact nhỏ.
  - `tests/test_csv_exporter.py` kiểm tra exact schema columns cho cả 6 CSV.
  - Tất cả schema có cột `mode`.
  - Test xác nhận null được ghi thành field rỗng, không ghi `0` giả.
  - Test xác nhận các cột bắt buộc cho event decisions, rolling house timeseries,
    plug metrics, house summary, threshold trace và experiment runs.
- Blocker:
  - Không có.
- Next step:
  - Chạy Prompt 07 để mở rộng rolling/summary metrics runtime nếu cần.

### 2026-06-06T08:15:16+07:00 - Phase P06: Rolling và summary metrics

- Trạng thái: `done`
- File đã sửa:
  - `src/aggregator/csv_exporter.py`
  - `tests/test_csv_exporter.py`
  - `docs/requirments/hiereb_completion_progress_log.md`
- Lệnh đã chạy:
  - `python3 -m compileall src/aggregator tests/test_csv_exporter.py`
  - `.venv/bin/python -m pytest tests/test_csv_exporter.py tests/test_aggregator.py -q`
  - `.venv/bin/python -m pytest -q`
  - `rg -n "rolling_insufficient_data|ROLLING_MIN_VALID_TIMESTAMPS|ROLLING_WINDOW_SECONDS|rmse_house_error|p95_abs_house_error|full_tx" src/aggregator tests/test_csv_exporter.py tests/test_aggregator.py`
- Kết quả:
  - Compile: pass.
  - Metrics/export tests: `18 passed in 0.17s`.
  - Full suite: `150 passed in 0.78s`.
  - `rg` verification: có constants và tests cho rolling event-time, P95/RMSE và full_tx.
- Acceptance evidence:
  - Rolling metrics dùng cửa sổ event-time `(t - 3600, t]`, không dùng row-count hoặc wall-clock.
  - `rolling_insufficient_data` dựa trên số timestamp hợp lệ trong window: `< 60`.
  - Test có fixture timestamp không đều và event biên `t - 3600` với error lớn để chứng minh boundary bị loại.
  - Summary metrics tính `RMSE_H`, `MAE_H`, `P95_H`, `MAX_H` trên `E_H(t)`.
  - Plug metrics tách reconstruction và prediction metrics.
  - `full_tx` reconstruction RMSE/MAE bằng `0`, prediction error vẫn tách riêng.
- Blocker:
  - Không có.
- Next step:
  - Chạy Prompt 08 để thêm sweep runner và metadata `run_id`.

## 7. Mẫu log cho các lần cập nhật sau

Sao chép mẫu dưới đây vào mục `6. Nhật ký cập nhật` sau mỗi prompt:

```text
### YYYY-MM-DD - Phase <ID>: <Tên phase>

- Trạng thái: <todo | in_progress | blocked | done | skipped>
- File đã sửa:
  - <path>
- Lệnh đã chạy:
  - <command>
- Kết quả:
  - <pass/fail/không chạy được + lý do>
- Acceptance evidence:
  - <bằng chứng ngắn gọn>
- Blocker:
  - <không có hoặc mô tả blocker>
- Next step:
  - <prompt/phase tiếp theo>
```
