"""Tiện ích InfluxDB: tạo client và đảm bảo bucket + retention policy."""
from __future__ import annotations

import logging

from influxdb_client import BucketRetentionRules, InfluxDBClient

from .config import Settings

log = logging.getLogger(__name__)


def make_client(s: Settings) -> InfluxDBClient:
    return InfluxDBClient(url=s.influx_url, token=s.influx_token, org=s.influx_org, timeout=10_000)


def ensure_bucket(client: InfluxDBClient, name: str, retention_s: int, org: str) -> str:
    """Tạo bucket nếu chưa có; cập nhật retention nếu khác cấu hình. Trả về created/updated/ok."""
    api = client.buckets_api()
    rules = BucketRetentionRules(type="expire", every_seconds=retention_s)
    bucket = api.find_bucket_by_name(name)
    if bucket is None:
        api.create_bucket(bucket_name=name, retention_rules=rules, org=org)
        log.info("Đã tạo bucket %s (retention %ss)", name, retention_s)
        return "created"
    current = bucket.retention_rules[0].every_seconds if bucket.retention_rules else 0
    if current != retention_s:
        bucket.retention_rules = [rules]
        api.update_bucket(bucket)
        log.info("Đã cập nhật retention bucket %s: %ss -> %ss", name, current, retention_s)
        return "updated"
    return "ok"


def query_raw(client: InfluxDBClient, s: Settings, start: str, stop: str = "now()",
              device: str | None = None, fields: list[str] | None = None):
    """Đọc measurement sensor_raw trong khoảng thời gian, trả về pandas DataFrame
    với cột _time, device_id, location và mỗi field là một cột."""
    import pandas as pd

    def _t(x: str) -> str:
        # '-1h' hoặc 'now()' giữ nguyên; thời điểm tuyệt đối ISO thì bọc time(v: ...)
        return x if x.startswith("-") or x.endswith(")") else f'time(v: "{x}")'

    dev = f'\n  |> filter(fn: (r) => r.device_id == "{device}")' if device else ""
    fld = ""
    if fields:
        cond = " or ".join(f'r._field == "{f}"' for f in fields)
        fld = f"\n  |> filter(fn: (r) => {cond})"
    flux = f'''
from(bucket: "{s.bucket_raw}")
  |> range(start: {_t(start)}, stop: {_t(stop)})
  |> filter(fn: (r) => r._measurement == "sensor_raw"){dev}{fld}
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> drop(columns: ["_start", "_stop", "_measurement"])
'''
    res = client.query_api().query_data_frame(flux, org=s.influx_org)
    if isinstance(res, list):
        res = pd.concat(res, ignore_index=True) if res else pd.DataFrame()
    if res.empty:
        return res
    res = res.drop(columns=[c for c in ("result", "table") if c in res.columns])
    res["_time"] = pd.to_datetime(res["_time"], utc=True)
    return res.sort_values("_time").reset_index(drop=True)


def ensure_all_buckets(client: InfluxDBClient, s: Settings) -> None:
    ensure_bucket(client, s.bucket_raw, s.raw_retention_s, s.influx_org)
    ensure_bucket(client, s.bucket_processed, s.processed_retention_s, s.influx_org)
