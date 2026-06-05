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
| P02 | `todo` | Prompt 02 | Predictor và missing prediction forced transmit | Chưa có |
| P03 | `todo` | Prompt 03 | Event decisions và reconstruction metric | Chưa có |
| P04A | `todo` | Prompt 04 | Uniform active-budget baseline | Chưa có |
| P04B | `todo` | Prompt 05 | HierEB allocator budget/timing/sigma_floor | Chưa có |
| P05 | `todo` | Prompt 06 | Sáu CSV output đúng schema | Chưa có |
| P06 | `todo` | Prompt 07 | Rolling metrics và summary metrics | Chưa có |
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
| P0-03 | P0 | `todo` | P02 | Missing prediction luôn forced transmit | `pytest tests/test_predictor.py tests/test_plug_state.py -q` | Không dùng last value làm fallback metric |
| P0-04 | P0 | `todo` | P03 | Metric chính là `actual_load` vs `reconstructed_load` | `pytest tests/test_aggregator.py tests/test_simulator_main.py -q` | Không dùng residual predictor làm house RMSE |
| P0-05 | P0 | `todo` | P03 | `full_tx` có reconstruction error bằng 0 | `pytest tests/test_aggregator.py -q` | Predictor error có thể ghi riêng |
| P0-06 | P0 | `todo` | P04A | Uniform dùng active-budget: `delta_p = Delta_H / N_budget_active` | `pytest tests/test_plug_state.py tests/test_simulator_main.py -q` | Cần metadata active plug |
| P0-07 | P0 | `todo` | P04B | HierEB allocator bảo toàn budget, không inflate do `delta_min` | `pytest tests/test_allocator.py -q` | Có assertion tổng threshold |
| P1-01 | P1 | `done` | P01 | Invalid value, duplicate và sort stable được xử lý | `pytest tests/test_loader.py tests/test_partition_debs_by_house.py -q` | Duplicate giữ `id` lớn nhất |
| P1-02 | P1 | `todo` | P04B | `sigma_floor` cố định từ warm-up toàn house | `pytest tests/test_allocator.py -q` | Không dùng percentile runtime |
| P1-03 | P1 | `todo` | P04B | Threshold update có `threshold_version` và `effective_after_time` | `pytest tests/test_allocator.py tests/test_simulator_main.py -q` | Tránh retroactive decision |
| P1-04 | P1 | `todo` | P04A/P04B | Inactive plug và reactivation không phá budget | `pytest tests/test_allocator.py tests/test_plug_state.py -q` | Active set cần rõ |
| P1-05 | P1 | `todo` | P06 | Rolling metrics tính theo event-time window | `pytest tests/test_aggregator.py -q` | Cửa sổ 1 giờ nếu không có yêu cầu khác |
| P1-06 | P1 | `todo` | P05 | Export đủ 6 CSV: events, metrics_per_plug, metrics_per_household, metrics_per_house, experiments_summary, threshold_history | `pytest tests/test_exports.py -q` | Schema phải ổn định |
| P2-01 | P2 | `todo` | P07 | Sweep runner có config grid và metadata `run_id` | `pytest tests/test_experiment_runner.py -q` | Dùng để so sánh Pareto |
| P2-02 | P2 | `todo` | P08 | Có SVG Pareto/transmission/RMSE từ output CSV | `pytest tests/test_dashboard.py -q` | Nếu không có test UI, ghi smoke evidence |
| P2-03 | P2 | `todo` | P08 | Dashboard hiển thị `P95`, `Delta_H`, `sigma_floor`, `active_plug_count` | `pytest tests/test_dashboard.py -q` | Post-hoc là đủ |
| P2-04 | P2 | `todo` | P06 | Plug metrics có `event_count`, `transmitted_count`, `suppressed_count`, `missing_prediction_count` | `pytest tests/test_aggregator.py tests/test_exports.py -q` | Cần cho audit |

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

- Trạng thái: `todo`
- File dự kiến: `src/ml_hiereb/predictor.py`, `src/simulator/plug_state.py`,
  `src/simulator/main.py`, `tests/test_predictor.py`, `tests/test_plug_state.py`,
  `tests/test_simulator_main.py`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P01 done

### P03 - Event decisions và reconstruction metric

- Trạng thái: `todo`
- File dự kiến: `src/simulator/main.py`, `src/aggregator/main.py`,
  `src/aggregator/db_writer.py`, `scripts/init_db.sql`,
  `tests/test_aggregator.py`, `tests/test_simulator_main.py`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P02 done

### P04A - Uniform active-budget baseline

- Trạng thái: `todo`
- File dự kiến: `src/simulator/plug_state.py`, `src/simulator/main.py`,
  `config/settings.py`, `tests/test_plug_state.py`, `tests/test_simulator_main.py`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P03 done

### P04B - HierEB allocator

- Trạng thái: `todo`
- File dự kiến: `src/ml_hiereb/allocator.py`, `src/ml_hiereb/main.py`,
  `src/simulator/main.py`, `tests/test_allocator.py`, `tests/test_simulator_main.py`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P03 và P04A đủ nền metric

### P05 - Sáu CSV output

- Trạng thái: `todo`
- File dự kiến: export scripts/module hiện có, `tests/test_exports.py`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P03 và P04B done

### P06 - Rolling và summary metrics

- Trạng thái: `todo`
- File dự kiến: `src/aggregator/main.py`, `src/aggregator/db_writer.py`,
  export scripts/module hiện có, `tests/test_aggregator.py`, `tests/test_exports.py`
- Test đã chạy: chưa có
- Kết quả: chưa có
- Blocker: không có
- Next step: chờ P05 đủ schema

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
