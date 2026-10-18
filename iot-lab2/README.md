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
| `tools/fake_device.py` | Thiết bị giả lập để test khi không chạy Wokwi |
| `tests/` | Unit test cho validator |
| `docker-compose.yml` | InfluxDB, Grafana, Mosquitto |
| `grafana/provisioning/` | Datasource InfluxDB tự cấu hình |

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
| field | `latency_ms` | float | Thời điểm ingest nhận − `ts` của thiết bị |
| time | `ts` | ms | Thời điểm đo tại thiết bị |

Measurement `pipeline_stats` (tag `component=ingest`): `received`, `written`, `invalid`, `duplicate`, `lost`, `reboot`, `loss_pct`, `lat_avg_ms`, `lat_p95_ms`, `lat_max_ms`, `write_avg_ms`, `backlog`.

**Bucket `iot_processed`** (retention 30 ngày): dùng cho kết quả tiền xử lý.

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
- **Độ trễ đo được phụ thuộc đồng hồ thiết bị.** Khi Wokwi chạy chậm hơn thời gian thực (xem chỉ số % tốc độ mô phỏng), đồng hồ ESP32 bị trôi và `latency_ms` sẽ tăng dần. Nên chạy trên máy đủ mạnh và ghi nhận hiện tượng này trong báo cáo.
- Không commit file `.env` lên GitHub.
