# Bài thực hành 2 – Thu thập, lưu trữ và tiền xử lý dữ liệu IoT

Pipeline: **ESP32 (Wokwi) → MQTT → Python ingest → InfluxDB → tiền xử lý → Grafana**

```
ESP32 + DHT22 + PIR + LDR (Wokwi)
        │  JSON, mỗi 2 s
        ▼
MQTT broker (broker.hivemq.com | Mosquitto cục bộ)
        │  topic iot/<site>/<location>/<device_id>/telemetry
        ▼
ingest/mqtt_to_influx.py  ── validate ── phát hiện trùng/mất gói ── đo độ trễ
        │
        ▼
InfluxDB 2.7  bucket iot_raw (7 ngày)  ──►  tiền xử lý  ──►  bucket iot_processed (30 ngày)
        │
        ▼
Grafana (http://localhost:3000)
```

## Cấu trúc thư mục

| Thư mục / file | Nội dung |
|---|---|
| `firmware/` | `sketch.ino`, `diagram.json`, `libraries.txt` cho Wokwi |
| `ingest/mqtt_to_influx.py` | Subscribe MQTT, kiểm tra dữ liệu, ghi InfluxDB |
| `ingest/validator.py` | Luật kiểm tra hợp lệ và theo dõi `seq` |
| `common/` | Đọc `.env`, tạo bucket và retention policy |
| `preprocess/pipeline.py` | Các bước tiền xử lý (làm sạch, outlier, resample, đặc trưng, chuẩn hoá) |
| `preprocess/run.py` | Đọc `iot_raw`, chạy tiền xử lý, ghi `iot_processed`, xuất báo cáo |
| `analysis/latency_report.py` | Thống kê độ trễ, tỷ lệ mất gói, vẽ biểu đồ |
| `tools/fake_device.py` | Thiết bị giả lập để test khi không chạy Wokwi |
| `tools/build_dashboard.py` | Sinh lại file dashboard Grafana |
| `tests/` | Unit test cho validator và tiền xử lý |
| `docker-compose.yml` | InfluxDB, Grafana, Mosquitto |
| `grafana/provisioning/` | Datasource InfluxDB và dashboard tự cấu hình |
| `reports/` | Kết quả xuất ra: CSV, JSON, biểu đồ PNG dùng cho báo cáo |

## Yêu cầu

- Docker Desktop (hoặc Docker Engine + Compose v2), hoặc GitHub Codespaces nếu không cài Docker được
- Python 3.8 trở lên (khuyến nghị 3.10+; với 3.8 hãy chạy `python -m pip install --upgrade pip` trước)
- Tài khoản Wokwi

## Cài đặt và chạy

### 1. Cấu hình

```bash
cp .env.example .env
```

Mở `.env` và đổi ít nhất `INFLUX_TOKEN` thành một chuỗi dài ngẫu nhiên. Sửa `MQTT_TOPIC` cho khớp với `SITE`, `LOCATION`, `DEVICE_ID` trong `firmware/sketch.ino`.

### 2. Khởi động InfluxDB và Grafana

```bash
docker compose up -d
docker compose ps        # influxdb phải ở trạng thái healthy
```

- InfluxDB UI: http://localhost:8086 (đăng nhập bằng `INFLUX_ADMIN_USER` / `INFLUX_ADMIN_PASSWORD`)
- Grafana: http://localhost:3000 (datasource **InfluxDB** đã được cấu hình sẵn)

> Biến `DOCKER_INFLUXDB_INIT_*` chỉ có tác dụng ở lần chạy đầu. Nếu đổi token hoặc mật khẩu sau đó, chạy `docker compose down -v` để xoá dữ liệu cũ rồi khởi động lại.

### 3. Cài thư viện Python

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pytest -q                          # kiểm tra validator
```

### 4. Chạy firmware trên Wokwi

1. Mở project Wokwi của Bài 1, dán đè 3 file trong `firmware/`.
2. Sửa `DEVICE_ID` (nên chứa MSSV) và các hằng số ở đầu `sketch.ino` nếu cần.
3. Chạy mô phỏng. Serial Monitor phải hiện `[NTP] OK`, `[MQTT] ... OK` và các dòng `[PUB] {...}`.

### 5. Chạy ingest

```bash
python -m ingest.mqtt_to_influx            # ghi vào InfluxDB
python -m ingest.mqtt_to_influx --dry-run  # chỉ in line protocol, không cần InfluxDB
```

Khi khởi động, script tự tạo bucket `iot_processed` và áp retention cho cả hai bucket theo `.env`. Mỗi `STATS_INTERVAL_S` giây, script in một dòng `STATS` và ghi vào measurement `pipeline_stats`.

### Test cục bộ không cần Wokwi

```bash
docker compose --profile local up -d       # bật thêm Mosquitto
# trong .env: MQTT_HOST=localhost
python -m ingest.mqtt_to_influx
python tools/fake_device.py --host localhost --interval 1 --faults
```

Cờ `--faults` chèn ngẫu nhiên spike, mất gói, gói trùng, JSON hỏng và NaN để kiểm tra khả năng xử lý lỗi.

### 6. Tiền xử lý

Cần có ít nhất 30–60 phút dữ liệu thô trước khi chạy.

```bash
python -m preprocess.run --start -1h                  # xử lý 1 giờ gần nhất (IQR)
python -m preprocess.run --start -1h --method zscore  # dùng Z-score
python -m preprocess.run --start -30m --loop 60       # chạy liên tục mỗi 60 s để dashboard có dữ liệu mới
```

Các bước (file `preprocess/pipeline.py`):

| Bước | Cách làm | Lý do |
|---|---|---|
| 1. Làm sạch | Sắp xếp theo thời gian, bỏ bản ghi trùng thời điểm | Dữ liệu có thể đến không theo thứ tự hoặc bị gửi lặp |
| 2. Outlier | Tính độ lệch so với trung vị trượt 5 mẫu, rồi áp IQR (k = 1,5) hoặc Z-score (\|z\| > 3); yêu cầu lệch tối thiểu 2 °C / 5 %RH | IQR/Z-score áp trực tiếp lên chuỗi có bước nhảy (đổi mức nhiệt trên Wokwi) sẽ báo nhầm; độ lệch so với trung vị chỉ làm nổi bật spike đơn lẻ |
| 3. Resample | Trung bình theo cửa sổ 1 phút; motion thành tỷ lệ thời gian có chuyển động | Đưa dữ liệu về lưới thời gian đều, giảm nhiễu và dung lượng |
| 4. Giá trị thiếu | Nội suy theo thời gian cho khoảng trống ≤ 5 cửa sổ; khoảng dài hơn để trống | Nội suy qua quãng mất dữ liệu dài sẽ tạo số liệu giả |
| 5. Đặc trưng | `*_roll5` (rolling mean 5 cửa sổ), `*_delta` (chênh lệch giữa hai cửa sổ) | Xu hướng và tốc độ thay đổi, dùng cho cảnh báo hoặc mô hình |
| 6. Chuẩn hoá | `*_norm` (Min-Max về [0, 1]), `*_z` (Z-score) | Đưa các đại lượng khác đơn vị về cùng thang đo |

Kết quả được ghi vào bucket `iot_processed`, measurement `sensor_processed`. Trong thư mục `reports/` có:
- `<device>_preprocess.png`: biểu đồ trước/sau xử lý, đánh dấu outlier và điểm nội suy
- `<device>_report.json`: số bản ghi, trùng, outlier, cửa sổ trống/đã nội suy, tham số chuẩn hoá
- `<device>_outlier_compare.csv`: so sánh số outlier giữa IQR và Z-score
- `<device>_raw.csv`, `<device>_processed.csv`: dữ liệu trước và sau

### 7. Dashboard Grafana

Mở cổng 3000, đăng nhập bằng `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`, vào **Dashboards → IoT Lab 2 → IoT Lab 2 – Giám sát cảm biến**. Dashboard tự làm mới mỗi 5 giây, gồm 3 hàng:

1. **Real-time**: giá trị mới nhất và biểu đồ dữ liệu thô.
2. **Sau tiền xử lý**: dữ liệu thô và sau xử lý chồng lên nhau, dữ liệu chuẩn hoá, delta. Cần chạy `preprocess.run --loop` để hàng này cập nhật liên tục.
3. **Sức khoẻ pipeline**: tỷ lệ mất gói, số gói nhận/loại/mất/trùng, độ trễ từng gói và theo chu kỳ.

Nếu Grafana đã chạy từ trước khi có file dashboard, chạy `docker compose restart grafana`.

### 8. Đánh giá độ trễ và lưu trữ

```bash
python -m analysis.latency_report --start -1h
docker exec iot-influxdb du -sh /var/lib/influxdb2/engine     # dung lượng dữ liệu trên đĩa
```

Kết quả: `reports/latency_summary.csv` (số gói, tốc độ gói/giây, độ trễ min/trung bình/p50/p95/p99/max, tỷ lệ mất gói tính từ `seq`) và `reports/latency.png`.

## Định dạng dữ liệu

**Topic:** `iot/<site>/<location>/<device_id>/telemetry`. Ngoài ra có topic `.../status` nhận `online`/`offline` (MQTT Last Will).

**Payload:**

```json
{"device_id":"esp32-tung-01","location":"lab","seq":42,"ts":1760750000123,
 "temperature":27.4,"humidity":63.1,"light":312.5,"light_raw":1250,
 "motion":0,"rssi":-58,"uptime_s":84}
```

`ts` là epoch milliseconds (UTC) lấy từ NTP trên thiết bị.

## Schema InfluxDB

**Bucket `iot_raw`** (retention 7 ngày), measurement `sensor_raw`:

| Loại | Tên | Kiểu | Ghi chú |
|---|---|---|---|
| tag | `device_id`, `location`, `site` | string | Dùng để lọc, được đánh index |
| field | `temperature`, `humidity`, `light` | float | °C, %RH, lux |
| field | `light_raw`, `motion`, `rssi`, `uptime_s`, `seq` | int | |
| field | `latency_ms` | float | Thời điểm ingest nhận − `ts` của thiết bị (bỏ trống nếu đồng hồ thiết bị lệch quá `MAX_CLOCK_SKEW_MS`) |
| field | `device_ts` | int | `ts` gốc do thiết bị gửi (ms) |
| field | `clock_offset_ms` | int | Giờ server − giờ thiết bị, để theo dõi đồng hồ bị trôi |
| time | `ts` hoặc giờ server | ms | Thời điểm đo tại thiết bị; dùng giờ server khi đồng hồ thiết bị lệch |

Measurement `pipeline_stats` (tag `component=ingest`): `received`, `written`, `invalid`, `duplicate`, `lost`, `reboot`, `loss_pct`, `lat_avg_ms`, `lat_p95_ms`, `lat_max_ms`, `write_avg_ms`, `backlog`.

**Bucket `iot_processed`** (retention 30 ngày), measurement `sensor_processed`:

| Loại | Tên | Ghi chú |
|---|---|---|
| tag | `device_id`, `location`, `method` | `method` = iqr hoặc zscore |
| field | `temperature`, `humidity`, `light` | Trung bình mỗi cửa sổ, đã loại outlier và nội suy |
| field | `motion_ratio` | Tỷ lệ thời gian có chuyển động trong cửa sổ |
| field | `*_roll5`, `*_delta` | Đặc trưng: rolling mean, chênh lệch |
| field | `*_norm`, `*_z` | Chuẩn hoá Min-Max và Z-score |
| field | `n_samples` (int), `filled` (int 0/1) | Số mẫu thô trong cửa sổ; 1 = giá trị được nội suy |
| time | đầu cửa sổ | Mỗi cửa sổ 1 phút |

## Xử lý lỗi, trùng lặp và mất mát

| Tình huống | Cách xử lý |
|---|---|
| JSON hỏng, thiếu trường, sai kiểu, NaN | Loại bỏ, ghi lý do vào `logs/rejected.jsonl` |
| Giá trị ngoài dải đo của cảm biến (vd độ ẩm > 100) | Loại bỏ ở bước validate |
| Giá trị trong dải nhưng bất thường (spike) | Vẫn lưu dữ liệu thô; bước tiền xử lý phát hiện bằng IQR / Z-score |
| Gói trùng (cùng `seq`) | Bỏ qua. Nếu lọt qua, InfluxDB cũng ghi đè vì trùng tag + timestamp |
| Mất gói (`seq` nhảy cóc) | Đếm vào `lost`, tính `loss_pct` |
| Thiết bị khởi động lại | Nhận ra khi `seq` / `uptime_s` quay về gần 0 |
| InfluxDB tạm thời lỗi | Giữ tối đa 10 000 điểm trong bộ nhớ, ghi bù khi DB hoạt động lại |
| Mất kết nối MQTT | paho-mqtt tự kết nối lại (1–30 s) |

## Lưu ý đã biết

- **Wokwi không kết nối được tới `localhost`**, vì vậy firmware dùng broker public. Broker public không có xác thực, ai cũng đọc được topic, chỉ phù hợp cho bài thực hành.
- **PubSubClient chỉ publish QoS 0**, nên phát hiện mất gói dựa vào `seq` chứ không dựa vào cơ chế QoS.
- **Độ trễ đo được phụ thuộc đồng hồ thiết bị.** Wokwi thường chạy chậm hơn thời gian thực (xem % tốc độ mô phỏng) nên đồng hồ ESP32 bị trôi, có thể lệch tới hàng giờ. Firmware đồng bộ NTP lại mỗi 5 phút. Ở phía server, nếu `|giờ nhận − ts|` vượt `MAX_CLOCK_SKEW_MS` (mặc định 10 s), gói được lưu theo giờ server, trường `latency_ms` bị bỏ trống và bộ đếm `clock_skew` tăng; độ lệch luôn được lưu ở trường `clock_offset_ms` để phân tích.
- **Wokwi tạm dừng mô phỏng khi tab bị ẩn.** Mở Wokwi ở một cửa sổ trình duyệt riêng và để hiển thị trong lúc thu dữ liệu.
- Không commit file `.env` lên GitHub.
