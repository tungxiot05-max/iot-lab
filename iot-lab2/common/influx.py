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


def ensure_all_buckets(client: InfluxDBClient, s: Settings) -> None:
    ensure_bucket(client, s.bucket_raw, s.raw_retention_s, s.influx_org)
    ensure_bucket(client, s.bucket_processed, s.processed_retention_s, s.influx_org)
