# Huong Dan Demo Va Inspect He Thong HierEB

Tai lieu nay dung cho buoi demo/bao cao: chay he thong, xem dashboard, doc ket qua CSV/SVG, va inspect tung thanh phan khi can giai thich pipeline.

## 1. Muc Tieu Demo

He thong HierEB mo phong viec giam tan suat truyen du lieu tu smart plug:

- `simulator` doc du lieu DEBS CSV, replay theo thoi gian, gui message len Kafka.
- `ml-hiereb` train predictor, publish prediction va threshold cho tung plug.
- `aggregator` consume sensor data, tinh metric cap house, ghi vao TimescaleDB.
- `Grafana` doc TimescaleDB de hien thi bieu do.
- `scripts/demo_report.sh` chay 3 che do so sanh: `full_tx`, `uniform`, `hiereb`.

Ket qua can trinh bay:

- Transmission Rate (`tr`): ty le plug that su truyen du lieu. Cang thap thi cang tiet kiem bang thong/nang luong.
- RMSE: sai so tong quat cua house-level load.
- `e_h`: house error theo tung timestamp.
- P95 `|e_h|`: sai so duoi nguong 95% mau.
- Bieu do `Actual vs Reconstructed Load`, `Transmission Rate Trend`, `Rolling RMSE`.

## 2. Chuan Bi Truoc Demo

Dung tu thu muc root cua project:

```bash
cd /home/chicongn/education/all-semesters/sixth-semester/hiereb
```

Kiem tra file cau hinh va data:

```bash
cp .env.example .env
ls -lh data/house-1.csv
head -2 data/house-1.csv
```

Neu da co `.env` roi thi khong can copy lai. Cac bien quan trong:

- `SUPPRESSION_MODE`: `full_tx`, `uniform`, hoac `hiereb`.
- `RUN_ID`: ten lan chay de tach metric trong database.
- `HOUSE_IDS`: vi du `[1]`.
- `DATA_FILE`: vi du `house-1.csv`.
- `REPLAY_SPEED`: toc do replay.
- `EPSILON_H`: budget sai so house; `0.05` nghia la 5% mean house load.
- `TAU`: chu ky reallocation cua HierEB theo data-time seconds.
- `DATASET_PRESET`: `one_house`, `five_houses`, `all_house`, hoac `custom`.
- `DATA_WINDOW`: `one_day`, `five_days`, `a_week`, hoac `all`.
- `STREAM_READ_CHUNK_SIZE`: so row giu trong RAM tren moi file house khi replay.

### Chuan bi data nhieu house

Khong nen de simulator doc truc tiep file `all-house` lon. Tach tung cua so
thoi gian thanh mot file cho moi house bang lenh streaming sau:

```bash
python scripts/partition_debs_by_house.py \
  --input data/all-house/one-day.csv \
  --output-dir data/partitioned/one-day \
  --property 1
```

Neu data goc da tach san thanh `data/house-*.csv`, tao partition mot ngay cho
tat ca house dang co bang lenh:

```bash
python scripts/partition_debs_by_house.py \
  --input data/house-*.csv \
  --output-dir data/partitioned/one-day \
  --property 1 --max-duration-seconds 86400 --skip-malformed --overwrite
```

`--skip-malformed` duoc dung vi bo du lieu local hien co mot dong hong dinh
dang trong `data/house-16.csv`; so dong bo qua duoc ghi vao `manifest.json`.

Tuong tu cho cua so lon hon:

```bash
python scripts/partition_debs_by_house.py \
  --input data/all-house/five-days.csv \
  --output-dir data/partitioned/five-days \
  --property 1

python scripts/partition_debs_by_house.py \
  --input data/all-house/a-week.csv \
  --output-dir data/partitioned/a-week \
  --property 1
```

Simulator se doc tung partition va merge theo timestamp, khong `concat` toan
bo 40 house vao mot DataFrame. ML-HierEB fit tung partition lan luot.

Vi du `.env` de chay mot house:

```env
DATASET_PRESET=one_house
DATA_WINDOW=one_day
ONE_HOUSE_ID=1
```

Vi du `.env` de chay nam house:

```env
DATASET_PRESET=five_houses
DATA_WINDOW=one_day
FIVE_HOUSE_IDS=[0,1,2,10,11]
```

Vi du `.env` de chay tat ca partition da tao:

```env
DATASET_PRESET=all_house
DATA_WINDOW=one_day
```

Chay kiem tra mode `hiereb` theo preset da chon:

```bash
DATASET_PRESET=five_houses DATA_WINDOW=one_day \
FIVE_HOUSE_IDS='[0,1,2,10,11]' bash scripts/verify_e2e.sh hiereb
```

## 3. Demo Nhanh Mot Lenh

Lenh chinh:

```bash
E2E_RUNTIME_SECONDS=45 HOUSE_ID=1 KEEP_STACK=1 bash scripts/demo_report.sh
```

Lenh nay se:

1. Start Kafka, TimescaleDB, Grafana.
2. Chay `full_tx`.
3. Chay `uniform`.
4. Chay `hiereb` voi `ml-hiereb`.
5. Ghi summary vao `results/demo_*/e2e_summary.csv`.
6. Export CSV chi tiet vao `results/demo_*/export/`.
7. Sinh bieu do SVG vao `results/demo_*/charts/`.
8. Giu stack chay de mo Grafana live.

Sau khi chay xong, mo:

```text
http://localhost:3000
user: admin
password: hiereb_pass
dashboard: HierEB Report Dashboard
```

Trong dashboard, chon `run_id` va `house_id` o dau trang. Nen demo theo thu tu:

1. Chon run `full_tx`: chi ra `Average TR` gan `1.0`, tuc la truyen toan bo.
2. Chon run `uniform`: chi ra `Average TR` giam, day la baseline suppression don gian.
3. Chon run `hiereb`: chi ra `Average TR`, `RMSE`, `P95 |e_h|`, va trend theo thoi gian.
4. Mo panel `Actual vs Reconstructed Load` de noi ve reconstruction.
5. Mo panel `Transmission Rate Trend` de noi ve muc giam truyen.
6. Mo panel `Rolling RMSE (1 minute)` de noi ve trade-off giua tiet kiem truyen va sai so.

## 4. Artifact De Dua Vao Bao Cao

Tim thu muc demo moi nhat:

```bash
ls -dt results/demo_* | head -n 1
```

Xem nhanh artifact:

```bash
bash scripts/inspect_demo_artifacts.sh
```

Cac file quan trong:

- `e2e_summary.csv`: bang so sanh nhanh 3 mode.
- `export/run_summary.csv`: metric tong hop theo run.
- `export/timeseries_house_<HOUSE_ID>_<RUN_ID>.csv`: du lieu theo timestamp.
- `export/minute_metrics_house_<HOUSE_ID>_<RUN_ID>.csv`: metric bucket 1 phut.
- `charts/chart_avg_tr.svg`: bieu do transmission rate theo mode.
- `charts/chart_rmse.svg`: bieu do RMSE theo mode.
- `charts/chart_tr_1m_comparison.svg`: trend transmission rate.
- `charts/chart_rmse_1m_comparison.svg`: trend RMSE.
- `charts/chart_actual_vs_reconstructed_<RUN_ID>.svg`: actual vs reconstructed cho tung run.

## 5. Inspect Toan He Thong

Chay:

```bash
bash scripts/inspect_system.sh
```

Script nay kiem tra:

- Danh sach service Docker Compose.
- Trang thai container.
- Process dang chay trong container.
- Kafka topics.
- Tong hop run trong TimescaleDB.
- Metric moi nhat theo house.
- Health API cua Grafana.
- Artifact demo moi nhat.

De chay tat ca script inspect:

```bash
LOG_LINES=80 bash scripts/inspect_all.sh
```

`LOG_LINES` dieu khien so dong log moi nhat moi script in ra.

## 6. Inspect Kafka

Chay:

```bash
bash scripts/inspect_kafka.sh
```

Dung khi muon chung minh Kafka la backbone message bus.

No in ra:

- Status `kafka` va `kafka-init`.
- Topic list: `hiereb.sensor_data`, `hiereb.predictions`, `hiereb.thresholds`, `hiereb.variance`.
- Topic detail: partition, replication factor.
- End offsets tung topic.
- Consumer groups: `hiereb-aggregator`, `hiereb-ml`.
- Consumer lag cua aggregator va ML.
- Log Kafka va Kafka init.

Y nghia khi demo:

- `hiereb.sensor_data` tang offset khi simulator dang replay.
- `hiereb.variance` tang trong `uniform` va `hiereb`.
- `hiereb.predictions` va `hiereb.thresholds` tang khi `ml-hiereb` dang chay.
- Consumer lag thap nghia la consumer theo kip producer.

## 7. Inspect TimescaleDB

Chay:

```bash
bash scripts/inspect_timescaledb.sh
```

Dung khi muon chung minh du lieu da duoc ghi va co the tinh metric bao cao.

No in ra:

- Health cua PostgreSQL/TimescaleDB.
- Version database.
- Danh sach table.
- Danh sach hypertable.
- `run_summary`: rows, avg TR, RMSE, P95, max error.
- 10 rows metric moi nhat.
- Tong quan `threshold_log`.
- Log database.

Bang chinh:

- `house_metrics`: metric cap house theo timestamp; day la nguon cua Grafana va CSV report.
- `threshold_log`: lich su threshold neu service ghi threshold vao DB.
- `experiment_runs`, `run_results`: danh cho mo rong quan ly experiment.

## 8. Inspect Grafana

Chay:

```bash
bash scripts/inspect_grafana.sh
```

Dung khi muon chung minh dashboard duoc provision tu source code.

No in ra:

- Status container Grafana.
- Health API `/api/health`.
- File datasource provisioning.
- File dashboard provider.
- Dashboard JSON.
- Cac title trong dashboard.
- Log Grafana.

File lien quan:

- `grafana/provisioning/datasources/timescaledb.yml`
- `grafana/provisioning/dashboards/provider.yml`
- `grafana/dashboards/hiereb_main.json`

## 9. Inspect Simulator

Chay:

```bash
bash scripts/inspect_simulator.sh
```

Dung khi giai thich dau vao pipeline.

Simulator lam viec:

- Doc `DATA_PATH/DATA_FILE`.
- Group readings theo `(house_id, timestamp)`.
- Chon transmit/suppress theo `SUPPRESSION_MODE`.
- Publish message vao `hiereb.sensor_data`.
- Trong `uniform` va `hiereb`, publish variance update vao `hiereb.variance`.

Script in ra:

- Trang thai service.
- Env runtime: mode, run id, data file, replay speed, epsilon, tau.
- Offset cua `hiereb.sensor_data`.
- Offset cua `hiereb.variance`.
- Run rows da ghi vao DB.
- Log simulator, trong do co `simulator_progress` va `overall_tr`.

## 10. Inspect Aggregator

Chay:

```bash
bash scripts/inspect_aggregator.sh
```

Dung khi giai thich xu ly stream va ghi metric.

Aggregator lam viec:

- Consume `hiereb.sensor_data`.
- Parse tung message theo timestamp.
- Tinh `actual_load`, `pred_load`, `e_h`, `tr`, `plug_count`, `transmitted_count`.
- Ghi vao `house_metrics`.

Script in ra:

- Trang thai service.
- Env runtime: DB host, run id, batch size.
- Consumer group `hiereb-aggregator`.
- 20 metric moi nhat trong DB.
- Tong hop metric theo run/house.
- Log aggregator, trong do co `aggregator_progress`.

## 11. Inspect ML-HierEB

Chay:

```bash
bash scripts/inspect_ml_hiereb.sh
```

Dung khi giai thich phan thuat toan.

ML-HierEB lam viec:

- Load data training.
- Fit `TimeSlicePredictor`.
- Publish prediction batch vao `hiereb.predictions`.
- Consume variance update tu `hiereb.variance`.
- Chay allocator HierEB theo chu ky `TAU`.
- Publish threshold vao `hiereb.thresholds`.

Script in ra:

- Trang thai service.
- Env runtime: epsilon, tau, batch interval.
- Consumer group `hiereb-ml`.
- Offset cua `hiereb.predictions`, `hiereb.thresholds`, `hiereb.variance`.
- Summary `threshold_log`.
- Log co cac event `training_split`, `error_budget_resolved`, `predictions_published`, `thresholds_published`.

## 12. Kich Ban Thuyet Trinh Goi Y

Mo dau:

```text
He thong nay mo phong smart plug streaming. Muc tieu la giam so lan truyen du lieu nhung van giu sai so house-level trong muc chap nhan duoc.
```

Giai thich architecture:

```text
Simulator la nguon event, Kafka la message bus, ML-HierEB sinh prediction va threshold, Aggregator tinh metric, TimescaleDB luu time-series, Grafana hien thi dashboard.
```

Chay demo:

```bash
E2E_RUNTIME_SECONDS=45 HOUSE_ID=1 KEEP_STACK=1 bash scripts/demo_report.sh
```

Trong luc/hoac sau khi chay, inspect:

```bash
bash scripts/inspect_kafka.sh
bash scripts/inspect_timescaledb.sh
bash scripts/inspect_grafana.sh
bash scripts/inspect_simulator.sh
bash scripts/inspect_aggregator.sh
bash scripts/inspect_ml_hiereb.sh
bash scripts/inspect_demo_artifacts.sh
```

Ket luan bang metric:

```text
full_tx la baseline truyen 100%. uniform va hiereb giam transmission rate. Diem can danh gia la trade-off giua avg TR va RMSE/P95 error.
```

Nhan xet senior nen noi thang:

```text
Neu run hien tai cho thay uniform co avg TR thap hon hiereb va RMSE tuong duong, HierEB chua phai la ket qua toi uu. Gia tri cua project luc nay nam o pipeline hoan chinh, kha nang do luong, dashboard, va khung tuning. Buoc tiep theo la sweep EPSILON_H, TAU, predictor quality, va policy reallocation de tim trade-off tot hon.
```

## 13. Troubleshooting Nhanh

Neu Grafana khong mo:

```bash
bash scripts/inspect_grafana.sh
docker compose ps grafana
```

Neu khong co data trong dashboard:

```bash
bash scripts/inspect_timescaledb.sh
bash scripts/inspect_aggregator.sh
```

Neu Kafka topic khong co offset:

```bash
bash scripts/inspect_kafka.sh
bash scripts/inspect_simulator.sh
```

Neu HierEB khong publish threshold:

```bash
LOG_LINES=200 bash scripts/inspect_ml_hiereb.sh
```

Neu muon chay lai demo sach ve run ID nhung giu DB:

```bash
E2E_RUNTIME_SECONDS=45 HOUSE_ID=1 KEEP_STACK=1 bash scripts/demo_report.sh
```

Moi lan chay se tao `RUN_ID` moi, nen khong can xoa database de so sanh.
