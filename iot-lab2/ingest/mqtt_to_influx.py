"""Subscribe MQTT -> validate -> ghi InfluxDB (measurement sensor_raw).

Chạy:   python -m ingest.mqtt_to_influx            (ghi vào InfluxDB)
        python -m ingest.mqtt_to_influx --dry-run  (chỉ in line protocol, không cần InfluxDB)
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import statistics
import threading
import time
import uuid
from collections import deque
from pathlib import Path

import paho.mqtt.client as mqtt
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from common.config import ROOT, Settings, load_settings
from common.influx import ensure_all_buckets, make_client
from ingest.validator import SeqTracker, ValidationError, validate

log = logging.getLogger("ingest")

MEASUREMENT_RAW = "sensor_raw"
MEASUREMENT_STATS = "pipeline_stats"
BACKLOG_MAX = 10_000          # số điểm tối đa giữ lại khi InfluxDB tạm thời lỗi
REJECT_LOG = ROOT / "logs" / "rejected.jsonl"


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def to_point(rec: dict, latency_ms: float) -> Point:
    p = (
        Point(MEASUREMENT_RAW)
        .tag("device_id", rec["device_id"])
        .tag("location", rec["location"])
        .tag("site", rec["site"])
        .time(rec["ts"], WritePrecision.MS)
        .field("latency_ms", float(latency_ms))
    )
    # Ép kiểu cố định: InfluxDB từ chối nếu cùng field lúc float lúc int
    for key in ("temperature", "humidity", "light"):
        if key in rec:
            p = p.field(key, float(rec[key]))
    for key in ("seq", "light_raw", "motion", "rssi", "uptime_s"):
        if key in rec:
            p = p.field(key, int(rec[key]))
    return p


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * q
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


class Ingestor:
    def __init__(self, settings: Settings, dry_run: bool = False) -> None:
        self.s = settings
        self.dry_run = dry_run
        self.lock = threading.Lock()
        self.tracker = SeqTracker()
        self.backlog: deque[Point] = deque(maxlen=BACKLOG_MAX)
        self.counters = dict(received=0, written=0, invalid=0, duplicate=0, out_of_order=0,
                             lost=0, reboot=0, write_errors=0)
        self.window_latency: list[float] = []   # device -> ingest (ms)
        self.window_write: list[float] = []     # thời gian ghi DB (ms)
        self.started = time.time()

        self.client = None
        self.write_api = None
        if not dry_run:
            self.client = make_client(settings)
            ensure_all_buckets(self.client, settings)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

        REJECT_LOG.parent.mkdir(parents=True, exist_ok=True)

    # ---------- Ghi dữ liệu ----------
    def _write(self, points: list[Point]) -> bool:
        if self.dry_run:
            for p in points:
                print(p.to_line_protocol(), flush=True)
            return True
        try:
            self.write_api.write(bucket=self.s.bucket_raw, org=self.s.influx_org, record=points)
            return True
        except Exception as e:  # noqa: BLE001 - mọi lỗi mạng/DB đều đưa vào backlog
            self.counters["write_errors"] += 1
            log.warning("Ghi InfluxDB lỗi (%s). Giữ %d điểm trong backlog", e.__class__.__name__,
                        len(self.backlog) + len(points))
            return False

    def _flush_backlog(self) -> None:
        while self.backlog:
            batch = [self.backlog.popleft() for _ in range(min(500, len(self.backlog)))]
            if not self._write(batch):
                self.backlog.extendleft(reversed(batch))
                return
            self.counters["written"] += len(batch)
            log.info("Đã ghi bù %d điểm từ backlog", len(batch))

    def _track_rejected(self, payload: bytes) -> None:
        """Gói bị loại vẫn đã tới nơi: cập nhật seq để không bị tính nhầm là mất gói trên mạng."""
        try:
            d = json.loads(payload)
            dev, seq, up = d["device_id"], d["seq"], d.get("uptime_s")
            if isinstance(dev, str) and isinstance(seq, int) and not isinstance(seq, bool):
                res = self.tracker.check(dev, seq, up if isinstance(up, int) else None)
                if res.status == "gap":
                    self.counters["lost"] += res.lost
        except (ValueError, KeyError, TypeError):
            pass

    # ---------- Xử lý message ----------
    def handle(self, topic: str, payload: bytes) -> None:
        t_recv = now_ms()
        with self.lock:
            self.counters["received"] += 1
            try:
                rec = validate(payload, topic, t_recv)
            except ValidationError as e:
                self.counters["invalid"] += 1
                log.warning("Loại gói không hợp lệ trên %s: %s", topic, e)
                with REJECT_LOG.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"recv_ms": t_recv, "topic": topic, "reason": str(e),
                                        "payload": payload.decode("utf-8", "replace")},
                                       ensure_ascii=False) + "\n")
                self._track_rejected(payload)
                return

            res = self.tracker.check(rec["device_id"], rec["seq"], rec.get("uptime_s"))
            if res.status == "duplicate":
                self.counters["duplicate"] += 1
                log.info("Bỏ gói trùng %s seq=%d", rec["device_id"], rec["seq"])
                return
            if res.status == "gap":
                self.counters["lost"] += res.lost
                log.warning("Mất %d gói của %s trước seq=%d", res.lost, rec["device_id"], rec["seq"])
            elif res.status == "reboot":
                self.counters["reboot"] += 1
                log.warning("%s khởi động lại (seq=%d)", rec["device_id"], rec["seq"])
            elif res.status == "out_of_order":
                self.counters["out_of_order"] += 1

            latency = t_recv - rec["ts"]
            point = to_point(rec, latency)

            if self.backlog:
                self._flush_backlog()
            t0 = time.perf_counter()
            ok = self._write([point])
            write_ms = (time.perf_counter() - t0) * 1000
            if ok:
                self.counters["written"] += 1
                self.window_latency.append(latency)
                self.window_write.append(write_ms)
                log.debug("OK %s seq=%d latency=%dms write=%.1fms", rec["device_id"], rec["seq"],
                          latency, write_ms)
            else:
                self.backlog.append(point)

    # ---------- Thống kê pipeline ----------
    def emit_stats(self) -> None:
        with self.lock:
            lat, wr = self.window_latency, self.window_write
            self.window_latency, self.window_write = [], []
            c = dict(self.counters)
            backlog = len(self.backlog)

        # Tỷ lệ mất gói trên mạng = gói không tới / (gói không tới + gói tới nơi, không tính bản trùng)
        arrived = c["received"] - c["duplicate"]
        expected = arrived + c["lost"]
        loss_pct = 100.0 * c["lost"] / expected if expected else 0.0
        stats = {
            **c,
            "backlog": backlog,
            "loss_pct": round(loss_pct, 3),
            "lat_avg_ms": round(statistics.fmean(lat), 1) if lat else 0.0,
            "lat_p95_ms": round(percentile(lat, 0.95), 1),
            "lat_max_ms": round(max(lat), 1) if lat else 0.0,
            "write_avg_ms": round(statistics.fmean(wr), 2) if wr else 0.0,
            "msgs_in_window": len(lat),
        }
        log.info("STATS %s", json.dumps(stats))

        p = Point(MEASUREMENT_STATS).tag("component", "ingest").time(now_ms(), WritePrecision.MS)
        for k, v in stats.items():
            p = p.field(k, float(v))
        if self.dry_run:
            print(p.to_line_protocol(), flush=True)
        elif not self._write([p]):
            pass  # thống kê không cần backlog

    def close(self) -> None:
        if self.backlog and not self.dry_run:
            self._flush_backlog()
        if self.client:
            self.client.close()


def build_mqtt(s: Settings, ing: Ingestor) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"ingest-{uuid.uuid4().hex[:8]}", clean_session=True)
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def on_connect(c, _u, _flags, reason_code, _props):
        if reason_code.is_failure:
            log.error("MQTT kết nối thất bại: %s", reason_code)
            return
        log.info("MQTT đã kết nối %s:%d, subscribe '%s' (QoS %d)", s.mqtt_host, s.mqtt_port,
                 s.mqtt_topic, s.mqtt_qos)
        c.subscribe(s.mqtt_topic, qos=s.mqtt_qos)

    def on_disconnect(_c, _u, _flags, reason_code, _props):
        log.warning("MQTT mất kết nối (%s), đang tự kết nối lại...", reason_code)

    def on_message(_c, _u, msg):
        try:
            ing.handle(msg.topic, msg.payload)
        except Exception:  # noqa: BLE001 - không để một gói lỗi làm dừng chương trình
            log.exception("Lỗi không mong đợi khi xử lý gói trên %s", msg.topic)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    return client


def main() -> None:
    ap = argparse.ArgumentParser(description="MQTT -> InfluxDB ingest")
    ap.add_argument("--dry-run", action="store_true", help="Không ghi DB, in line protocol ra màn hình")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    s = load_settings() if not args.dry_run else _settings_for_dry_run()
    ing = Ingestor(s, dry_run=args.dry_run)
    client = build_mqtt(s, ing)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    client.connect_async(s.mqtt_host, s.mqtt_port, keepalive=30)
    client.loop_start()
    log.info("Ingest đang chạy%s. Ctrl+C để dừng.", " (dry-run)" if args.dry_run else "")
    try:
        while not stop.wait(s.stats_interval_s):
            ing.emit_stats()
    finally:
        log.info("Đang dừng...")
        ing.emit_stats()
        client.loop_stop()
        client.disconnect()
        ing.close()


def _settings_for_dry_run() -> Settings:
    """Dry-run không cần token InfluxDB."""
    import os
    os.environ.setdefault("INFLUX_TOKEN", "dry-run")
    return load_settings()


if __name__ == "__main__":
    main()
