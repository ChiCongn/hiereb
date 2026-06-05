# HierEB - Thứ tự triển khai và prompt để hoàn thành requirements

Ngày tạo: 2026-06-05
Nguồn: `docs/requirments/hiereb_codebase_audit_report.md`
Progress log: `docs/requirments/hiereb_completion_progress_log.md`

Mục tiêu của file này là biến báo cáo audit thành một chuỗi việc có thể giao
cho Codex/agent theo từng vòng. Mỗi prompt bên dưới nên được chạy độc lập theo
thứ tự. Sau mỗi prompt, agent phải cập nhật progress log, ghi test đã chạy và
không đánh dấu hoàn thành nếu acceptance criteria chưa đạt.

## 1. Nguyên tắc thực thi

1. Sửa theo thứ tự P0 -> P1 -> P2.
2. Không làm dashboard/sweep trước khi data contract, decision contract và metric
   contract đúng.
3. Mỗi vòng chỉ sửa một nhóm logic có ranh giới rõ.
4. Mỗi vòng phải có test mới hoặc test cũ được cập nhật để phản ánh requirements
   mới.
5. Mỗi vòng phải chạy test liên quan; nếu không chạy được, ghi rõ lý do vào
   progress log.
6. Không xóa hoặc revert thay đổi không liên quan.
7. Sau mỗi vòng, cập nhật:
   `docs/requirments/hiereb_completion_progress_log.md`.

## 2. Thứ tự tổng thể

| Phase | Mục tiêu | Mức ưu tiên | Chặn bởi |
|---|---|---|---|
| 0 | Baseline audit snapshot và test hiện tại | Prep | Không |
| 1 | Data contract: `property=1`, window, invalid, duplicate, sort | P0 | Phase 0 |
| 2 | Prediction/suppression decision contract | P0 | Phase 1 |
| 3 | Reconstruction metric và event-level decisions | P0 | Phase 2 |
| 4 | Uniform và HierEB allocator đúng budget/timing | P0/P1 | Phase 3 |
| 5 | 6 CSV outputs đúng schema | P1 | Phase 3, 4 |
| 6 | Test suite mới theo checklist | P1 | Phase 1-5 |
| 7 | Sweep runner, Pareto charts, dashboard update | P2 | Phase 5, 6 |
| 8 | README/docs final và verification run | Final | Phase 7 |

## 3. Prompt theo từng phase

### Prompt 00 - Baseline snapshot và progress log

Mục tiêu: xác nhận trạng thái hiện tại trước khi sửa code.

```text
Bạn là Codex trong repo HierEB. Hãy tạo baseline trước khi implement:

1. Đọc:
   - docs/requirments/hiereb_codebase_audit_report.md
   - docs/requirments/hiereb_full_requirements.md
   - docs/requirments/hiereb_verification_checklist.md
   - docs/requirments/hiereb_completion_progress_log.md

2. Chạy:
   - git status --short
   - .venv/bin/python -m pytest -q

3. Không sửa source code.

4. Cập nhật progress log:
   - Điền baseline test result
   - Ghi ngày/giờ bắt đầu
   - Đánh dấu Phase 0 = done nếu test chạy được

Acceptance:
   - Progress log có baseline entry
   - Không sửa file code ngoài progress log
```

Verification:

```bash
git status --short
.venv/bin/python -m pytest -q
```

### Prompt 01 - Data contract: property, windows, invalid, duplicate, sort

Mục tiêu: sửa P0/P1 ở loader/config/docs liên quan dữ liệu.

```text
Bạn là Codex trong repo HierEB. Hãy implement data contract theo requirements.

Phạm vi chính:
   - config/settings.py
   - .env.example
   - src/simulator/loader.py
   - src/ml_hiereb/main.py nếu cần truyền warm-up/evaluation window
   - scripts/partition_debs_by_house.py
   - README.md
   - tests/test_loader.py
   - tests/test_partition_debs_by_house.py

Yêu cầu:
1. Default property phải là 1, vì DEBS gốc property=1 là load.
2. Comment/docs không được nói property=0 là instantaneous load.
3. Thêm cấu hình warm-up/evaluation tuyệt đối:
   - WARMUP_START=2013-09-01 00:00:00 UTC
   - WARMUP_END=2013-09-07 23:59:59 UTC
   - EVAL_START=2013-09-08 00:00:00 UTC
   - EVAL_END=2013-09-14 23:59:59 UTC
   hoặc dạng timestamp seconds tương đương, miễn là deterministic.
4. Loader phải lọc:
   - property=1
   - house_id
   - time window
   - value >= 0
   - value finite
5. Duplicate key:
   (timestamp, house_id, household_id, plug_id, property)
   phải giữ event có id lớn nhất.
6. Sort stable theo:
   timestamp, house_id, household_id, plug_id, property, id
7. PlugReading hoặc event model phải giữ đủ id/property nếu cần để audit.
8. Ghi metadata counts cho invalid/duplicate nếu module hiện có chỗ phù hợp;
   nếu chưa có metadata writer, trả về stats object hoặc TODO rõ ràng kèm test
   cho logic count.

Test cần thêm/cập nhật:
   - property=1 là default
   - property=0 bị bỏ qua trong experiment chính
   - negative và NaN/Inf bị loại
   - duplicate giữ id lớn nhất
   - sort đúng 6 cột
   - warm-up/evaluation không overlap

Sau khi xong:
   - Chạy test liên quan
   - Cập nhật progress log Phase 1

Acceptance:
   - tests/test_loader.py pass
   - tests/test_partition_debs_by_house.py pass
   - Không còn default/comment property=0 là load
```

Verification:

```bash
.venv/bin/python -m pytest tests/test_loader.py tests/test_partition_debs_by_house.py -q
rg -n "property.*0.*load|PROPERTY_FILTER=0|--property 0" config .env.example README.md scripts src tests
```

### Prompt 02 - Predictor và missing prediction forced transmit

Mục tiêu: sửa P0 missing prediction và loại fallback last/0 trong decision.

```text
Bạn là Codex trong repo HierEB. Hãy sửa predictor/simulator để missing prediction
không bao giờ được suppress.

Phạm vi chính:
   - src/ml_hiereb/predictor.py
   - src/simulator/plug_state.py
   - src/simulator/main.py
   - tests/test_predictor.py
   - tests/test_plug_state.py
   - tests/test_simulator_main.py

Yêu cầu:
1. Predictor unknown plug phải trả missing prediction, không trả 0.0.
2. PlugState không được fallback prediction sang last_value hoặc 0.0 cho metric chính.
3. Nếu không có prediction tại timestamp:
   - decision = transmit
   - predicted_load = null
   - residual = null
   - reason = missing_prediction
   - is_forced_transmit = true
4. Nếu có plug global median thì được dùng làm fallback prediction.
5. last_transmitted_value/last_value chỉ được dùng debug, không dùng làm fallback
   prediction trong metric chính.
6. Prediction phải dự đoán timestamp/slot hiện tại.

Test cần thêm/cập nhật:
   - unknown plug -> missing prediction
   - missing timestamp prediction -> forced transmit
   - predicted_load null khi missing
   - last_value không được dùng làm prediction fallback
   - strict > vẫn giữ nguyên

Sau khi xong:
   - Chạy test liên quan
   - Cập nhật progress log Phase 2

Acceptance:
   - Missing prediction không thể suppress
   - Test cũ "missing prediction uses last observed value" bị sửa/xóa đúng
```

Verification:

```bash
.venv/bin/python -m pytest tests/test_predictor.py tests/test_plug_state.py tests/test_simulator_main.py -q
rg -n "return 0.0|last_value|missing_prediction|is_forced_transmit" src/simulator src/ml_hiereb tests
```

### Prompt 03 - Event decisions và reconstruction metric đúng

Mục tiêu: sửa P0 metric house error và tạo nền cho CSV event-level.

```text
Bạn là Codex trong repo HierEB. Hãy sửa metric contract để đo actual-vs-reconstructed
đúng theo requirements.

Phạm vi chính:
   - src/simulator/main.py
   - src/aggregator/main.py
   - src/aggregator/db_writer.py
   - scripts/init_db.sql nếu cần schema mới
   - tests/test_aggregator.py
   - tests/test_simulator_main.py

Yêu cầu:
1. Event message hoặc internal record phải giữ đủ:
   - actual_load
   - predicted_load nullable
   - reconstructed_load
   - transmitted
   - decision/reason
   - plug_status
   - is_forced_transmit
2. Khi suppress:
   reconstructed_load = predicted_load
3. Khi transmit:
   reconstructed_load = actual_load
4. House error:
   E_H(t) = sum(actual_load) - sum(reconstructed_load)
   trên plug có observed event trong batch t.
5. full_tx reconstruction RMSE phải bằng 0.
6. Không dùng prediction làm actual_load cho suppressed events trong metric.
7. Nếu aggregator không thể biết actual suppressed vì message cũ không gửi actual,
   hãy thay đổi simulator message cho experiment/evaluation path để có actual
   phục vụ offline metric, hoặc tạo event_decisions artifact trực tiếp từ simulator.
   Không được giữ logic e_h=0 cho suppressed events.

Test cần thêm/cập nhật:
   - all suppressed nhưng actual != prediction -> e_h != 0
   - full_tx -> e_h = 0
   - mixed transmit/suppress -> E_H đúng
   - reconstruction = prediction khi suppress
   - reconstruction = actual khi transmit

Sau khi xong:
   - Chạy test aggregator/simulator
   - Cập nhật progress log Phase 3

Acceptance:
   - tests không còn khẳng định all suppressed e_h = 0 do thiếu actual
   - house RMSE có thể tính từ E_H đúng
```

Verification:

```bash
.venv/bin/python -m pytest tests/test_aggregator.py tests/test_simulator_main.py -q
rg -n "actual_load \\+= snap.predicted|e_h contribution.*0|all_suppressed_e_h_is_zero" src tests
```

### Prompt 04 - Uniform mode theo Delta_H và active plugs

Mục tiêu: sửa uniform baseline để so sánh công bằng với HierEB.

```text
Bạn là Codex trong repo HierEB. Hãy refactor uniform mode theo requirements.

Phạm vi chính:
   - config/settings.py
   - src/simulator/main.py
   - src/simulator/plug_state.py
   - src/ml_hiereb/main.py nếu cần dùng chung active set
   - tests/test_simulator_main.py
   - tests/test_plug_state.py
   - tests/test_ml_main.py

Yêu cầu:
1. uniform không dùng fixed UNIFORM_DELTA trong experiment chính.
2. uniform_delta_H(t) = Delta_H / N_budget_active_plugs_H(t)
3. active_window_seconds = 3600 event-time seconds.
4. Plug không có event trong active window:
   - delta_p = 0
   - plug_status = inactive
5. Plug inactive reappear:
   - forced transmit
   - plug_status = reactivated
   - reason = inactive_reactivation, trừ khi missing prediction thì reason = missing_prediction
6. uniform threshold mới áp dụng cùng timing với HierEB:
   timestamp > allocation_time
7. Nếu cần giữ UNIFORM_DELTA, chỉ dùng cho legacy/debug mode và ghi rõ không phải baseline.

Test cần thêm:
   - uniform delta từ Delta_H/N active
   - active count thay đổi làm uniform delta thay đổi ở allocation kế tiếp
   - inactive reactivation forced transmit
   - missing prediction ưu tiên hơn inactive_reactivation

Sau khi xong:
   - Chạy tests liên quan
   - Cập nhật progress log Phase 4-uniform

Acceptance:
   - Không còn dùng settings.UNIFORM_DELTA trong baseline uniform decision
```

Verification:

```bash
.venv/bin/python -m pytest tests/test_simulator_main.py tests/test_plug_state.py tests/test_ml_main.py -q
rg -n "UNIFORM_DELTA|inactive_reactivation|active_window|N_budget_active" src tests config
```

### Prompt 05 - HierEB allocator đúng sigma_floor, budget, rolling variance, timing

Mục tiêu: sửa allocator theo công thức đầy đủ.

```text
Bạn là Codex trong repo HierEB. Hãy refactor HierEB allocator theo requirements.

Phạm vi chính:
   - src/ml_hiereb/allocator.py
   - src/ml_hiereb/main.py
   - src/simulator/kafka_listeners.py
   - tests/test_allocator.py
   - tests/test_ml_main.py

Yêu cầu:
1. Tính sigma_floor_H một lần từ warm-up:
   percentile_5({sigma_p > 0 trong house})
   fallback = 1.0 W
2. Giữ sigma_floor_H cố định trong evaluation.
3. Weight:
   sigma_p = sqrt(max(v_p, 0))
   w_p = max(sigma_p, sigma_floor_H)
4. Phân bổ:
   W_g = sum w_p trong household
   Delta_g = Delta_H * W_g / W_H
   delta_p = Delta_g * w_p / W_g
5. sum(delta_p) <= Delta_H, có assertion/tolerance.
6. Không dùng delta_min > 0 trong baseline.
7. Variance=0 vẫn nhận w_p=sigma_floor_H.
8. Cold start:
   - median household weight
   - median house weight
   - fallback 1.0 W
9. Rolling window = 1000 effective residual samples per plug.
10. Transmit update dùng residual thật.
11. Suppress update dùng delta_used^2/3 với delta_used là threshold cũ tại decision.
12. Inactive plugs nhận delta=0, không chiếm budget dương.
13. Threshold trace metadata nội bộ phải có:
    allocation_time, effective_after_time, threshold_version.
14. Threshold mới chỉ apply cho event timestamp > allocation_time.

Test cần thêm/cập nhật:
   - sigma_floor fixed từ warm-up
   - variance=0 vẫn có weight
   - no min delta làm vượt budget
   - sum delta <= Delta_H với epsilon nhỏ/nhiều plug
   - censored contribution đúng delta_used cũ
   - rolling 1000 samples
   - threshold timing > allocation_time

Sau khi xong:
   - Chạy allocator/ml tests
   - Cập nhật progress log Phase 4-hiereb

Acceptance:
   - Không còn hard-coded floor 0.1 làm delta_p
   - Có test bắt budget bound
```

Verification:

```bash
.venv/bin/python -m pytest tests/test_allocator.py tests/test_ml_main.py -q
rg -n "0\\.1|sigma_floor|threshold_version|effective_after_time|delta_used" src/ml_hiereb tests
```

### Prompt 06 - 6 CSV outputs đúng schema

Mục tiêu: tạo export/artifact layer đúng requirements.

```text
Bạn là Codex trong repo HierEB. Hãy implement 6 CSV outputs bắt buộc.

Phạm vi chính:
   - src/aggregator/
   - src/simulator/
   - src/ml_hiereb/
   - scripts/export_report_data.sh hoặc script Python export mới
   - scripts/init_db.sql nếu cần bảng mới
   - tests cho schema CSV

CSV bắt buộc:
1. experiment_runs.csv
2. event_decisions.csv
3. house_timeseries.csv
4. plug_metrics.csv
5. house_summary.csv
6. threshold_trace.csv

Yêu cầu chung:
   - Tất cả file có cột mode trực tiếp.
   - Granularity đúng theo requirements.
   - Null phải là null/empty CSV field, không ghi 0 giả.
   - Có schema tests kiểm tra columns.

Các cột đặc biệt bắt buộc:
   - event_decisions: plug_status, is_forced_transmit, reason
   - house_timeseries: rolling_tr_1h, rolling_rmse_1h, rolling_insufficient_data
   - plug_metrics: rmse_reconstruction, mae_reconstruction, rmse_prediction, mae_prediction,
     inactive_reactivation_count, missing_prediction_count
   - house_summary: p95_abs_house_error, max_abs_house_error, Delta_H, epsilon_ratio
   - threshold_trace: allocation_time, effective_after_time, threshold_version,
     trace_granularity, sigma_floor_used, delta_used_for_censored_update
   - experiment_runs: uniform_delta_initial, sigma_floor_used, sweep metadata

Sau khi xong:
   - Chạy tests schema/export
   - Cập nhật progress log Phase 5

Acceptance:
   - Có script tạo đủ 6 CSV từ một run/demo nhỏ hoặc từ in-memory test fixture
   - Test schema pass
```

Verification:

```bash
.venv/bin/python -m pytest tests -q
find results -name 'experiment_runs.csv' -o -name 'event_decisions.csv' -o -name 'house_timeseries.csv' -o -name 'plug_metrics.csv' -o -name 'house_summary.csv' -o -name 'threshold_trace.csv'
```

### Prompt 07 - Rolling metrics và summary metrics

Mục tiêu: hoàn tất metrics theo event-time.

```text
Bạn là Codex trong repo HierEB. Hãy implement rolling metrics và summary metrics
đúng requirements.

Phạm vi chính:
   - src/aggregator/
   - scripts/export_report_data.sh hoặc export Python mới
   - tests/test_aggregator.py
   - tests mới cho metrics

Yêu cầu:
1. Rolling window = event-time seconds, cửa sổ (t - 3600, t].
2. Không dùng số row cố định hoặc wall-clock.
3. rolling_insufficient_data = true nếu cửa sổ < 60 timestamp hợp lệ.
4. MAE_H, RMSE_H, P95_H, MAX_H đúng trên E_H(t).
5. Plug metrics tách reconstruction và prediction.
6. full_tx reconstruction RMSE = 0.

Sau khi xong:
   - Chạy tests metrics
   - Cập nhật progress log Phase 5-metrics

Acceptance:
   - Tests có fixture timestamp không đều để chứng minh dùng event-time
```

Verification:

```bash
.venv/bin/python -m pytest tests/test_aggregator.py tests -q
rg -n "time_bucket\\('1 minute'|rolling_insufficient_data|p95_abs_house_error|rmse_prediction" src scripts tests
```

### Prompt 08 - Sweep runner và run_id format

Mục tiêu: tạo thí nghiệm chính multi-point Pareto sweep.

```text
Bạn là Codex trong repo HierEB. Hãy implement sweep runner theo requirements.

Phạm vi chính:
   - config/settings.py
   - scripts/run_sweep.sh hoặc scripts/run_sweep.py
   - scripts/verify_e2e.sh nếu cần tích hợp
   - README.md
   - tests cho run_id/config nếu hợp lý

Yêu cầu:
1. epsilon_ratio_values mặc định:
   [0.01, 0.02, 0.05, 0.10, 0.20]
2. Sweep tối thiểu nếu thiếu tài nguyên:
   [0.02, 0.05, 0.10], reduced_sweep=true
3. full_tx chạy 1 lần, epsilon_ratio=null, is_sweep=false.
4. uniform chạy mỗi epsilon.
5. hiereb chạy mỗi epsilon.
6. run_id format:
   {sweep_id}_{mode}_house{house_id}_eps{epsilon_ratio_x100}
   ví dụ sweep01_hiereb_house0_eps005
7. Metadata ghi sweep_id, is_sweep, sweep_size, reduced_sweep.

Sau khi xong:
   - Chạy tests/script dry-run nếu có
   - Cập nhật progress log Phase 6

Acceptance:
   - Có thể sinh danh sách run plan deterministic mà không start Docker
   - Có thể chạy sweep thật qua script
```

Verification:

```bash
.venv/bin/python -m pytest tests -q
bash scripts/run_sweep.sh --dry-run || true
rg -n "epsilon_ratio_values|sweep_id|reduced_sweep|eps005|run_sweep" config scripts tests README.md
```

### Prompt 09 - Pareto SVG và dashboard update

Mục tiêu: cập nhật visualization theo output mới.

```text
Bạn là Codex trong repo HierEB. Hãy cập nhật SVG export và dashboard theo requirements.

Phạm vi chính:
   - scripts/generate_report_charts.py
   - grafana/dashboards/hiereb_main.json
   - README.md hoặc docs demo
   - tests nếu chart generator có test được

Yêu cầu SVG:
1. Pareto TR vs RMSE.
2. Pareto TR vs P95.
3. Encode epsilon_ratio trên mỗi điểm.
4. Time-series chi tiết chỉ cho epsilon_ratio=0.05.
5. Threshold trace/distribution cho hiereb.
6. Naming convention:
   - {run_group}_house{house_id}_{chart_name}.svg
   - {run_group}_house{house_id}_{mode}_{chart_name}.svg

Yêu cầu dashboard:
1. Post-hoc hoặc query corrected metrics.
2. Có đủ chart tối thiểu:
   - TR over time
   - actual vs reconstructed
   - house error với Delta_H
   - TR vs RMSE
   - TR vs P95
   - threshold distribution/trace
3. Không dùng hard-coded 50W thay Delta_H.
4. Dùng P95, không chỉ P90.

Sau khi xong:
   - Chạy chart generator trên fixture/export demo nhỏ
   - Cập nhật progress log Phase 7

Acceptance:
   - Có SVG Pareto và threshold trace
   - Dashboard không còn dùng violation threshold hard-code 50W cho kết luận chính
```

Verification:

```bash
.venv/bin/python -m pytest tests -q
python3 scripts/generate_report_charts.py --help
rg -n "Pareto|P95|Delta_H|50W|threshold_trace|epsilon_ratio" scripts grafana README.md
```

### Prompt 10 - Final verification và docs cleanup

Mục tiêu: kiểm tra toàn bộ requirements/checklist và cập nhật tài liệu.

```text
Bạn là Codex trong repo HierEB. Hãy chạy final verification sau khi các phase
trước đã xong.

Phạm vi:
   - docs/requirments/hiereb_verification_checklist.md
   - docs/requirments/hiereb_completion_progress_log.md
   - README.md
   - docs/demo_system_guide.md nếu cần

Yêu cầu:
1. Chạy full unit tests.
2. Chạy e2e smoke nếu môi trường Docker sẵn sàng.
3. Chạy demo-smoke theo house 0, property 1.
4. Nếu có data, chạy reduced sweep.
5. Kiểm tra đủ 6 CSV.
6. Kiểm tra SVG Pareto.
7. Tick checklist chỉ khi code + test + output đúng.
8. Cập nhật README để không còn hướng dẫn property=0.
9. Cập nhật progress log final status.

Acceptance:
   - Unit tests pass
   - Smoke run tạo đủ artifacts
   - Checklist phản ánh đúng trạng thái thật, không tick ảo
```

Verification:

```bash
.venv/bin/python -m pytest -q
bash scripts/verify_e2e.sh all
bash scripts/run_sweep.sh --reduced
find results -name '*.csv' | sort
find results -name '*.svg' | sort
```

## 4. Ma trận phụ thuộc

| Task | Phụ thuộc | Vì sao |
|---|---|---|
| CSV schema | Event decisions + metrics | Không có event-level truth thì CSV chỉ là vỏ. |
| Sweep | Uniform/HierEB đúng budget | Sweep sai baseline sẽ tạo Pareto vô nghĩa. |
| Dashboard | Corrected metrics + CSV | Dashboard đẹp trước metric đúng sẽ gây kết luận sai. |
| E2E final | P0/P1 fixes | E2E hiện pass theo logic cũ, cần test đúng requirements. |

## 5. Definition of Done

Một phase chỉ được đánh dấu done khi:

1. Code đã sửa đúng phạm vi.
2. Test liên quan đã được thêm/cập nhật.
3. Test liên quan pass.
4. Progress log đã ghi:
   - files changed,
   - tests run,
   - result,
   - blockers nếu có,
   - next step.
5. Không tạo regression rõ ràng ở phase trước.

Toàn bộ kế hoạch chỉ được coi là hoàn thành khi:

- `PROPERTY_FILTER=1` là default đúng.
- Missing prediction forced transmit.
- House reconstruction error đúng actual-vs-reconstructed.
- `full_tx` reconstruction RMSE = 0.
- Uniform dùng `Delta_H / N_budget_active_plugs`.
- HierEB có `sigma_floor_H`, budget assertion, threshold timing.
- Có đủ 6 CSV bắt buộc.
- Có sweep runner và Pareto charts.
- Verification checklist được tick bằng bằng chứng test/output.
