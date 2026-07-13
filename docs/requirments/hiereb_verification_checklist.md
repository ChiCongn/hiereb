# HierEB — Checklist Kiểm tra Hệ thống

> Dùng để verify từng phần của hệ thống đã implement.  
> Mỗi mục có thể tick độc lập. Tick = đã implement VÀ đã test.  
> Ngày: 2026-06-05

---

## Cách dùng checklist này

- **[ ]** = Chưa làm
- **[x]** = Đã làm và đúng
- **[~]** = Đã làm nhưng chưa đúng hoàn toàn / còn thiếu
- **[N/A]** = Không áp dụng cho phạm vi hiện tại

Mỗi mục tick được khi: (1) code đã viết, (2) unit test đã pass, (3) output đã kiểm tra bằng mắt hoặc assertion.

---

## PHẦN 0: Final verification 2026-06-06

Các mục dưới đây chỉ tick theo evidence của lần chạy final verification, không
thay thế toàn bộ checklist chi tiết phía sau.

- [x] Full unit tests pass: `.venv/bin/python -m pytest -q` → `158 passed in 1.23s`.
- [x] Docker E2E smoke chạy được trong môi trường hiện tại.
- [x] Demo-smoke house `0`, `property_filter=1` chạy đủ 3 mode và tạo artifact ở `results/demo_20260606_141359/`.
- [x] Demo-smoke `full_tx`: `473` rows, `avg_tr = 1`, `rmse = 0`.
- [x] Demo-smoke `uniform`: `473` rows, `avg_tr = 0.7296153535991592`.
- [x] Demo-smoke `hiereb`: `473` rows, `avg_tr = 0.5590637247796795`, `rmse = 0.192653469682006`.
- [x] 6 CSV bắt buộc sinh được bằng required exporter fixture ở `/tmp/hiereb_final_required_csv`.
- [x] SVG Pareto TR/RMSE và TR/P95 sinh được ở `/tmp/hiereb_final_required_charts`.
- [x] SVG threshold distribution/trace cho `hiereb` sinh được ở `/tmp/hiereb_final_required_charts`.
- [x] Reduced sweep thật chạy trên fixture house `0`, property `1`: `results/sweeps/finalverify/e2e_summary.csv`.
- [x] Reduced sweep có đúng 7 run: `full_tx` một lần, `uniform/hiereb` cho epsilon `[0.02, 0.05, 0.10]`.
- [x] README và demo guide không còn hướng dẫn dùng `property=0` cho load chính.
- [x] Runtime bug được sửa: ML reallocation chỉ publish allocation mới khi đã có event-time quan sát đủ tới mốc allocation.
- [x] Progress log đã ghi final status và evidence.
- [~] Demo flow chính `scripts/demo_report.sh` hiện vẫn xuất CSV legacy; 6 CSV bắt buộc đã được xác minh bằng `scripts/export_required_csv.py` và schema tests.

---

## PHẦN 1: Dataset và tiền xử lý

### 1.1 Đọc và lọc dữ liệu
- [ ] Đọc được file CSV DEBS với format đúng 7 cột: `id, timestamp, value, property, plug_id, household_id, house_id`
- [ ] Filter `property = 1` trước mọi xử lý tiếp theo
- [ ] Filter theo `house_id` được chỉ định
- [ ] Filter theo time window (`warmup_start <= timestamp <= warmup_end` hoặc `eval_start <= timestamp <= eval_end`)
- [ ] Loại event có `value < 0`, ghi count vào metadata
- [ ] Loại event có `value` không phải số hữu hạn (NaN, Inf), ghi count vào metadata
- [ ] Giữ nguyên load đột biến lớn (không winsorize trong metric chính)
- [ ] Bỏ qua event có timestamp ngoài cửa sổ đã chọn

### 1.2 Xử lý duplicate
- [ ] Phát hiện duplicate key: `(timestamp, house_id, household_id, plug_id, property)`
- [ ] Giữ event có `id` lớn nhất trong batch trùng key
- [ ] Ghi số event bị bỏ qua do duplicate vào metadata experiment

### 1.3 Sort và batch
- [ ] Sort toàn bộ event theo thứ tự: `timestamp, house_id, household_id, plug_id, property, id` (sort ổn định)
- [ ] Gom event có cùng timestamp thành một batch logic
- [ ] Metric cấp house chỉ tính sau khi xử lý xong toàn bộ load event của batch timestamp đó

### 1.4 Missing timestamp
- [ ] Plug không có event tại `t` không bị coi là suppress
- [ ] Missing event không tính vào denominator của TR
- [ ] Missing event không bị impute trong metric chính
- [ ] `actual_load_H(t)` chỉ tính trên plug có observed event hợp lệ tại batch `t`

---

## PHẦN 2: Predictor

### 2.1 Time-slice median predictor
- [ ] `slot_seconds = 300` (5 phút)
- [ ] Định nghĩa đúng: `slot_of_week(t) = (day_of_week(t), floor(seconds_since_midnight(t) / 300))`
- [ ] `day_of_week`: ISO weekday, 0 = Monday, 6 = Sunday
- [ ] `M[p, s] = median({ x_p(u) | u ∈ warm-up, slot_of_week(u) = s })` — chỉ dùng warm-up data
- [ ] Chỉ dùng load events có giá trị hợp lệ (value >= 0, finite) để tính median
- [ ] Predictor fit xong trước khi evaluation bắt đầu
- [ ] Predictor bị đóng băng trong evaluation — không update bằng evaluation data trong baseline

### 2.2 Fallback prediction
- [ ] Fallback 1: `M[p, slot_of_week(t)]` — plug-specific slot median
- [ ] Fallback 2: Plug global median trong warm-up (khi không có slot-specific median)
- [ ] Fallback 3: Không có prediction → decision = transmit, reason = missing_prediction
- [ ] `last_transmitted_value` không được dùng làm fallback prediction trong metric chính
- [ ] Cột `predicted_load` = `null` khi không có prediction (không ghi zero)

### 2.3 Thời điểm prediction
- [ ] Prediction tồn tại trước khi virtual plug agent quyết định transmit/suppress
- [ ] Prediction dự đoán timestamp/slot hiện tại, không phải next row

### 2.4 Prediction error metric (độc lập với suppression)
- [ ] `prediction_error_p(t) = x_p(t) - x_hat_p(t)` — tính trên tất cả event (kể cả bị suppress)
- [ ] `RMSE_pred_p` và `MAE_pred_p` ghi vào `plug_metrics.csv`
- [ ] Không nhầm prediction RMSE với reconstruction RMSE

---

## PHẦN 3: Suppression engine

### 3.1 Quyết định transmit/suppress
- [ ] `residual = x_p(t) - x_hat_p(t)` — có dấu, dùng cho thống kê
- [ ] `abs_residual = |residual|` — dùng cho quyết định
- [ ] `transmit` nếu `abs_residual > delta_p` (strict >, không phải >=)
- [ ] `suppress` nếu `abs_residual <= delta_p`
- [ ] Khi `abs_residual == delta_p`: suppress (nằm trong budget)

### 3.2 Reconstruction value
- [ ] Khi transmit: `x_tilde_p(t) = x_p(t)` (actual)
- [ ] Khi suppress: `x_tilde_p(t) = x_hat_p(t)` (prediction)
- [ ] Không ghi zero khi suppress
- [ ] Không dùng hold-last-value trong metric chính

### 3.3 Forced transmit
- [ ] Event missing prediction → forced transmit, `reason = missing_prediction`
- [ ] Plug reactivation (status = reactivated) → forced transmit, `reason = inactive_reactivation` (trừ khi cũng missing prediction thì `reason = missing_prediction`, `plug_status = reactivated`)
- [ ] Thứ tự ưu tiên reason: `invalid_value > missing_prediction > inactive_reactivation > normal`
- [ ] `is_forced_transmit = true` khi decision không qua rule `abs_residual > delta_p`

### 3.4 Hành vi từng mode
- [ ] `full_tx`: luôn transmit, `x_tilde_p = x_p`, không cần threshold check
- [ ] `uniform`: dùng `delta_uniform_H(t) = Delta_H / N_budget_active_plugs_H(t)`
- [ ] `hiereb`: dùng `delta_p(t)` từ allocator
- [ ] Cả `uniform` và `hiereb`: thiếu prediction → forced transmit, không suppress

### 3.5 Plug inactive
- [ ] Plug không có event trong `active_window_seconds = 3600` → `delta_p = 0`, `plug_status = inactive`
- [ ] Plug inactive reappear → `decision = transmit`, `plug_status = reactivated`
- [ ] Sau reactivation: plug được đưa lại nhóm active cho allocation cycle kế tiếp
- [ ] Khi reactivation có prediction: vẫn ghi `predicted_load`, `residual`, `abs_residual`, `threshold = 0.0`

---

## PHẦN 4: HierEB allocator

### 4.1 Error budget
- [ ] `Delta_H = epsilon_ratio * mean_house_load_warmup` (cách tính mặc định)
- [ ] Hoặc `Delta_H` khai báo trực tiếp bằng absolute Watt
- [ ] `mean_house_load_warmup` được ghi vào metadata

### 4.2 sigma_floor
- [ ] `sigma_floor_H = percentile_5({ sigma_p | sigma_p > 0, p ∈ house H, warm-up })`
- [ ] Nếu không có `sigma_p > 0` trong warm-up: `sigma_floor_H = 1.0 W`
- [ ] `sigma_floor_H` được tính **một lần** từ warm-up và giữ **cố định** trong suốt evaluation
- [ ] `sigma_floor_H` được ghi vào `experiment_runs.csv` (cột `sigma_floor_used`) và `threshold_trace.csv`

### 4.3 Công thức phân bổ
- [ ] `sigma_p = sqrt(max(v_p, 0))`
- [ ] `w_p = max(sigma_p, sigma_floor_H)`
- [ ] `W_g = sum_{p ∈ household g} w_p`
- [ ] `W_H = sum_g W_g`
- [ ] `Delta_g = Delta_H * W_g / W_H`
- [ ] `delta_p = Delta_g * w_p / W_g`
- [ ] Fallback khi `W_H = 0`: `delta_p = Delta_H / N_budget_active_plugs_H`
- [ ] Kiểm tra: `sum_p delta_p ≈ Delta_H` (sai số làm tròn chấp nhận được, ví dụ < 1e-6 W)

### 4.4 Residual variance
- [ ] Khởi tạo từ warm-up, chỉ dùng load events có prediction hợp lệ
- [ ] Plug có ít hơn `min_variance_samples = 30` mẫu → cold-start
- [ ] Cập nhật bằng rolling window 1000 effective samples trong evaluation
- [ ] Event transmit: dùng residual có dấu thực tế
- [ ] Event suppress: dùng `var_contribution = delta_used^2 / 3`, `delta_used` là threshold **đang có hiệu lực** tại thời điểm suppress (không phải threshold sắp được tính)
- [ ] Không circular dependency: censored contribution dùng `delta_p` cũ, không phải `delta_p` mới

### 4.5 Cold start
- [ ] Plug cold-start nhận median weight của plug khác cùng household
- [ ] Nếu không có: median weight của house
- [ ] Nếu không có: `1.0 W`
- [ ] Sau khi có weight thay thế, chạy qua công thức allocation bình thường
- [ ] Plug cold-start nhưng không có prediction → vẫn transmit

### 4.6 Variance = 0
- [ ] Plug có `v_p = 0` vẫn nhận `w_p = sigma_floor_H > 0` (không bị loại khỏi budget)
- [ ] `delta_p > 0` cho plug có variance = 0 (không đặt về 0)

### 4.7 Ràng buộc threshold
- [ ] `delta_p >= 0` cho mọi plug
- [ ] `sum_p delta_p <= Delta_H`
- [ ] `delta_p <= Delta_H` cho từng plug
- [ ] Không có `delta_min > 0` mặc định trong baseline

### 4.8 Thời điểm hiệu lực
- [ ] Allocation cycle tại `B` dùng data có `timestamp <= B`
- [ ] Threshold mới áp dụng cho event có `timestamp > B`
- [ ] `effective_after_time = allocation_time` trong `threshold_trace.csv`
- [ ] `threshold_version` là số nguyên tăng dần theo allocation cycle trong cùng `(run_id, mode, house_id)`
- [ ] Threshold version 0 (từ warm-up): `effective_after_time = eval_start - 1`, áp dụng từ event đầu tiên của evaluation
- [ ] Batch có `timestamp == allocation_time` vẫn dùng threshold cũ

### 4.9 Chu kỳ cập nhật
- [ ] `allocation_period = 300 giây event-time` (không phải wall-clock)
- [ ] Allocation period tính theo event timestamp, không theo thời gian thực

---

## PHẦN 5: Metrics collector

### 5.1 Transmission rate
- [ ] `TR_H = transmitted_load_events_H / total_observed_load_events_H`
- [ ] Missing events không tính vào denominator
- [ ] `full_tx` cho `TR_H = 1.0` nếu không có invalid events
- [ ] `TR_p = transmitted_events_p / total_observed_load_events_p` (cấp plug)
- [ ] `reduction_vs_full_tx = 1 - TR_mode` (vì `TR_full_tx = 1.0`)

### 5.2 House error
- [ ] `actual_load_H(t) = sum_p x_p(t)` — chỉ plug có observed event hợp lệ trong batch `t`
- [ ] `reconstructed_load_H(t) = sum_p x_tilde_p(t)` — cùng tập plug
- [ ] `E_H(t) = actual_load_H(t) - reconstructed_load_H(t)`

### 5.3 House metrics
- [ ] `MAE_H = mean_t(|E_H(t)|)`
- [ ] `RMSE_H = sqrt(mean_t(E_H(t)^2))` — RMSE của tổng load, KHÔNG phải trung bình RMSE plug
- [ ] `P95_H = percentile_95(|E_H(t)|)`
- [ ] `MAX_H = max_t(|E_H(t)|)`
- [ ] `full_tx` cho `RMSE_H = 0` (reconstruction RMSE, không phải prediction RMSE)

### 5.4 Plug metrics
- [ ] `MAE_p = mean(|x_p(t) - x_tilde_p(t)|)` — reconstruction MAE
- [ ] `RMSE_p = sqrt(mean((x_p(t) - x_tilde_p(t))^2))` — reconstruction RMSE
- [ ] `RMSE_pred_p = sqrt(mean((x_p(t) - x_hat_p(t))^2))` — prediction RMSE (metric phụ)
- [ ] `MAE_pred_p = mean(|x_p(t) - x_hat_p(t)|)` — prediction MAE (metric phụ)

### 5.5 Rolling metrics
- [ ] Cửa sổ `(t - 3600, t]` — event-time seconds, nửa mở
- [ ] `rolling_TR_H(t) = transmitted / total` trong cửa sổ
- [ ] `rolling_RMSE_H(t) = sqrt(mean(E_H(u)^2))` cho `u` trong cửa sổ
- [ ] Nếu cửa sổ < 60 timestamp hợp lệ: đánh dấu `rolling_insufficient_data = true`
- [ ] Không tính rolling bằng số row cố định hay wall-clock

---

## PHẦN 6: CSV export

### 6.1 experiment_runs.csv
- [ ] Một dòng cho mỗi `(run_id, mode, house_id)`
- [ ] Có đủ các cột bắt buộc (xem schema mục 8.1 trong tài liệu yêu cầu)
- [ ] `uniform_delta_initial = Delta_H / n_budget_active_plugs_initial`
- [ ] Tên cột phải là `uniform_delta_initial` (không phải `uniform_delta_value`) hoặc nếu dùng `uniform_delta_value` phải có documentation rõ là "giá trị khởi tạo"
- [ ] `sigma_floor_used` ghi đúng giá trị đã dùng (chỉ `hiereb`)
- [ ] `full_tx`: `epsilon_ratio = null`, `is_sweep = false`
- [ ] `run_id` format: `{sweep_id}_{mode}_house{house_id}_eps{epsilon_ratio_x100}` (ví dụ: `sweep01_hiereb_house0_eps005`)
- [ ] `is_smoke_test = true` cho demo-smoke run

### 6.2 event_decisions.csv
- [ ] Một dòng cho mỗi load event hợp lệ đã xử lý
- [ ] Có cột `mode` trực tiếp (không chỉ `run_id`)
- [ ] `predicted_load = null` khi không có prediction (không ghi zero)
- [ ] `residual = null` khi không có prediction
- [ ] `threshold = 0.0` khi plug inactive
- [ ] `reason` đúng theo thứ tự ưu tiên: `invalid_value > missing_prediction > inactive_reactivation > normal`
- [ ] `plug_status` đúng: `active`, `inactive`, `reactivated`
- [ ] `is_forced_transmit = true` cho mọi forced transmit event
- [ ] Khi `inactive_reactivation` và có prediction: vẫn ghi `predicted_load`, `residual`, `abs_residual`

### 6.3 house_timeseries.csv
- [ ] Một dòng cho mỗi batch timestamp của mỗi `(run_id, house_id)`
- [ ] Có cột `mode` trực tiếp
- [ ] `rolling_tr_1h` và `rolling_rmse_1h` tính đúng bằng event-time cửa sổ `(t-3600, t]`
- [ ] `rolling_insufficient_data = true` khi < 60 timestamp hợp lệ trong cửa sổ
- [ ] `actual_load_H` và `reconstructed_load_H` chỉ tính trên plug có observed event trong batch đó

### 6.4 plug_metrics.csv
- [ ] Một dòng cho mỗi `(run_id, plug_id)`
- [ ] Có cột `mode`
- [ ] Có `rmse_reconstruction`, `mae_reconstruction` (không nhầm với prediction)
- [ ] Có `rmse_prediction`, `mae_prediction` (metric phụ)
- [ ] Có `inactive_reactivation_count` và `missing_prediction_count`

### 6.5 house_summary.csv
- [ ] Một dòng cho mỗi `(run_id, mode, house_id)`
- [ ] Có cột `mode` (bắt buộc)
- [ ] Có đủ: `tr`, `reduction_vs_full_tx`, `rmse_house`, `mae_house`, `p95_abs_house_error`, `max_abs_house_error`
- [ ] `rmse_house` là RMSE của tổng load, không phải trung bình RMSE plug
- [ ] `mean_prediction_rmse_plug` là metric phụ, không phải metric chính
- [ ] `Delta_H` và `epsilon_ratio` được ghi vào từng dòng

### 6.6 threshold_trace.csv
- [ ] `hiereb`: một dòng cho mỗi `(run_id, house_id, household_id, plug_id, allocation_time)` (per-plug)
- [ ] `uniform`: một dòng cho mỗi `(run_id, house_id, allocation_time)` (per-house, không per-plug)
- [ ] Có cột `mode`
- [ ] `effective_after_time = allocation_time`
- [ ] `threshold_version` tăng dần đúng trong cùng `(run_id, mode, house_id)`
- [ ] Threshold version 0: `effective_after_time = eval_start - 1`
- [ ] `sigma_floor_used` ghi đúng giá trị cố định từ warm-up
- [ ] `delta_used_for_censored_update` ghi threshold đang dùng khi suppress (không phải threshold mới)
- [ ] `trace_granularity`: `per_plug` cho `hiereb`, `per_house` cho `uniform`
- [ ] Với `uniform`: `household_id`, `plug_id`, `residual_variance`, `weight` = `null`; `uniform_delta_value` được điền; `threshold = uniform_delta_value`

---

## PHẦN 7: SVG export

### 7.1 Charts bắt buộc (per-run-group)
- [ ] `{run_group}_house{house_id}_tr_over_time.svg` — rolling TR theo time, 3 mode
- [ ] `{run_group}_house{house_id}_error_over_time.svg` — house error theo time, 3 mode, kèm đường `+/- Delta_H`
- [ ] `{run_group}_house{house_id}_actual_vs_reconstructed.svg` — actual và reconstructed house load
- [ ] `{run_group}_house{house_id}_pareto_rmse.svg` — TR vs RMSE, uniform và hiereb, full_tx anchor
- [ ] `{run_group}_house{house_id}_pareto_p95.svg` — TR vs P95, uniform và hiereb, full_tx anchor

### 7.2 Charts per-mode
- [ ] `{run_group}_house{house_id}_hiereb_threshold_trace.svg` — threshold distribution/trace, hiereb only

### 7.3 Yêu cầu chart Pareto
- [ ] Điểm `full_tx` là anchor tại `(TR=1.0, RMSE=0)` hoặc `(TR=1.0, P95=0)`
- [ ] Mỗi điểm trên Pareto chart encode `epsilon_ratio` (qua label, color, hoặc annotation)
- [ ] Chart time-series chi tiết chỉ vẽ cho `epsilon_ratio = 0.05`

### 7.4 Naming
- [ ] Tên file SVG deterministic và nhất quán với naming convention
- [ ] Per-run-group chart: không có `{mode}` trong tên file
- [ ] Per-mode chart: có `{mode}` trong tên file

---

## PHẦN 8: Dashboard

### 8.1 Minimum 6 charts
- [ ] Chart 1: Transmission rate over time (rolling TR, 3 series theo mode)
- [ ] Chart 2: House actual vs reconstructed load (Watt vs time, theo mode)
- [ ] Chart 3: House error over time (`E_H(t)`, kèm đường `+/- Delta_H`)
- [ ] Chart 4: Pareto TR vs RMSE_H
- [ ] Chart 5: Pareto TR vs P95_H
- [ ] Chart 6: Threshold distribution/trace (hiereb only)

### 8.2 Tính chất dashboard
- [ ] Post-hoc: đọc từ CSV/database sau khi run kết thúc
- [ ] Không yêu cầu streaming connection từ replay engine
- [ ] Có thể load kết quả từ nhiều run để so sánh

---

## PHẦN 9: Testing

### 9.1 Unit tests

**Dataset filter:**
- [ ] Test: filter `property = 1` loại bỏ `property = 0`
- [ ] Test: loại value < 0, đếm đúng
- [ ] Test: loại non-finite value, đếm đúng
- [ ] Test: duplicate key giữ event có `id` lớn nhất
- [ ] Test: sort batch theo đúng 6 cột

**Predictor:**
- [ ] Test: slot median đúng cho warm-up data
- [ ] Test: fallback sang plug global median khi không có slot-specific
- [ ] Test: fallback sang transmit khi không có global median
- [ ] Test: prediction không dùng evaluation data để fit

**Suppression:**
- [ ] Test: `abs_residual > delta_p` → transmit
- [ ] Test: `abs_residual == delta_p` → suppress (strict >)
- [ ] Test: `abs_residual < delta_p` → suppress
- [ ] Test: missing prediction → transmit với `reason = missing_prediction`
- [ ] Test: inactive reactivation → transmit với `reason = inactive_reactivation`
- [ ] Test: reconstruction = prediction khi suppress
- [ ] Test: reconstruction = actual khi transmit

**Uniform threshold:**
- [ ] Test: `delta_uniform = Delta_H / N_active` cho N_active = 1, 5, 10
- [ ] Test: khi N_active giảm (plug inactive), `delta_uniform` tăng đúng tỷ lệ
- [ ] Test: threshold mới có hiệu lực từ event sau `allocation_time`

**HierEB allocator:**
- [ ] Test: `sum delta_p = Delta_H` (tolerance 1e-6)
- [ ] Test: cold-start plug nhận median weight của household
- [ ] Test: variance = 0 → plug vẫn nhận `w_p = sigma_floor > 0`
- [ ] Test: plug inactive → `delta_p = 0`
- [ ] Test: allocator hoạt động đúng với 1 household, nhiều plug
- [ ] Test: allocator hoạt động đúng với nhiều household
- [ ] Test: `W_H = 0` → phân bổ đều

**Metrics:**
- [ ] Test: TR = 1.0 khi full_tx
- [ ] Test: house RMSE = 0 khi full_tx
- [ ] Test: house RMSE là RMSE của tổng load, không phải trung bình plug RMSE
- [ ] Test: rolling TR với cửa sổ đúng event-time seconds
- [ ] Test: rolling window `(t-3600, t]` — nửa mở đúng

**Determinism:**
- [ ] Test: hai lần chạy cùng config sinh cùng `house_summary.csv` (binary identical hoặc float tolerance)

### 9.2 End-to-end test
- [ ] E2E với subset DEBS house 0 chạy thành công 3 mode
- [ ] `full_tx` E2E: `TR = 1.0`, reconstruction RMSE = 0
- [ ] `uniform` E2E: có event suppress khi threshold > 0 và prediction tồn tại
- [ ] `hiereb` E2E: có event suppress; `sum delta_p <= Delta_H` tại mọi allocation cycle
- [ ] E2E: số timestamp trong `house_timeseries.csv` giống nhau giữa 3 mode (cùng evaluation window)
- [ ] E2E: kết quả deterministic (chạy lại 2 lần cho cùng kết quả)
- [ ] Synthetic test: allocator với 1 house, 2 household, 2 plug/household — kiểm tra budget constraint
- [ ] Synthetic test: plug inactive và reactivation đúng luồng

### 9.3 Demo script
- [ ] Demo script chạy thành công end-to-end mà không có lỗi
- [ ] Demo hoàn thành trong < 10 phút (hoặc thời gian hợp lý đã định nghĩa)
- [ ] Output đủ 6 CSV file
- [ ] Output đủ SVG charts tối thiểu
- [ ] Metadata ghi đúng seed, windows, Delta_H, mode

---

## PHẦN 10: Thiết kế thí nghiệm

### 10.1 Sweep configuration
- [ ] Có thể chạy sweep với nhiều `epsilon_ratio` values
- [ ] `epsilon_ratio_values = [0.01, 0.02, 0.05, 0.10, 0.20]` (mặc định)
- [ ] `full_tx` chạy 1 lần trong sweep, không lặp theo epsilon
- [ ] `run_id` theo format `{sweep_id}_{mode}_house{house_id}_eps{epsilon_ratio_x100}`

### 10.2 Metadata sweep
- [ ] `sweep_id` gom các run thuộc cùng sweep
- [ ] `is_sweep = true` cho các run trong sweep
- [ ] `sweep_size` ghi đúng số điểm epsilon
- [ ] `reduced_sweep = true` khi dùng [0.02, 0.05, 0.10]
- [ ] `full_tx` có `epsilon_ratio = null`, `is_sweep = false`

### 10.3 Pareto chart cho sweep
- [ ] Chart Pareto vẽ đúng: mỗi điểm là một epsilon_ratio, series theo mode
- [ ] `full_tx` anchor tại `(TR=1.0, error=0)` hiển thị đúng
- [ ] `epsilon_ratio` được encode trên mỗi điểm (label/color/annotation)

---

## PHẦN 11: Determinism và reproducibility

- [ ] Seed mặc định `42` cho mọi thứ random trong pipeline
- [ ] Sort event ổn định (stable sort) trong cùng timestamp
- [ ] Hai lần chạy cùng config → cùng output (không có non-deterministic operations)
- [ ] `experiment_runs.csv` ghi đầy đủ để reproduce: seed, windows, property_mapping, predictor config, `Delta_H`, allocation settings
- [ ] Nếu có thành phần random trong tương lai: nhận seed rõ ràng, báo cáo variance qua nhiều lần chạy

---

## PHẦN 12: Metadata và completeness

- [ ] Mỗi experiment có `run_id` duy nhất
- [ ] Metadata ghi `is_smoke_test = true` cho demo-smoke, `false` cho thí nghiệm chính
- [ ] Metadata ghi số event bị loại (invalid value, duplicate)
- [ ] Replay không kết thúc khi còn thiếu metric của batch cuối
- [ ] Kết quả không được coi là hoàn tất nếu thiếu summary theo mode
- [ ] Không có mode nào chạy trên cửa sổ thời gian khác mode còn lại trong cùng experiment

---

## PHẦN 13: Tính nhất quán toàn hệ thống

- [ ] Cột `mode` có mặt trong: `event_decisions.csv`, `house_timeseries.csv`, `plug_metrics.csv`, `house_summary.csv`, `threshold_trace.csv`
- [ ] `run_id` nhất quán trong tất cả file output của cùng một run
- [ ] `house_id` nhất quán giữa các file
- [ ] `timestamp` định nghĩa nhất quán (Unix seconds, UTC)
- [ ] Tất cả Watt values là float, không phải int
- [ ] `null` và `NA` được xử lý nhất quán trong tất cả file CSV

---

## PHẦN 14: Báo cáo — tránh sai lầm phổ biến

- [ ] Không viết "predictor tốt vì full_tx có reconstruction RMSE = 0" — đây là lỗi logic
- [ ] Prediction RMSE và reconstruction RMSE được báo cáo riêng, rõ ràng
- [ ] Không viết "hiereb có budget lớn hơn uniform" — hai mode dùng cùng `Delta_H`
- [ ] Câu hỏi nghiên cứu: "Với cùng tổng budget, phân bổ theo residual variability có tốt hơn phân bổ đều không?"
- [ ] `uniform_delta_initial` không bị diễn giải là threshold cố định suốt run
- [ ] Nếu dùng reduced sweep: ghi rõ `reduced_sweep = true`, không diễn giải quá mạnh về Pareto frontier
- [ ] Nếu lợi thế HierEB chỉ xuất hiện ở một vùng epsilon: kết luận phải nêu điều kiện đó
- [ ] Nếu hiereb kém hơn uniform ở mọi điểm: kết luận rõ "công thức hiện tại chưa chứng minh được lợi thế"
