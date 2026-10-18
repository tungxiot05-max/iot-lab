"""Đọc cấu hình từ file .env ở thư mục gốc dự án."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(text: str) -> int:
    """'7d' -> 604800 giây. '0' nghĩa là giữ vĩnh viễn."""
    text = text.strip().lower()
    if text in ("0", "inf", "infinite"):
        return 0
    m = re.fullmatch(r"(\d+)\s*([smhdw])", text)
    if not m:
        raise ValueError(f"Thời lượng không hợp lệ: {text!r} (ví dụ 7d, 12h, 30m)")
    return int(m.group(1)) * _UNITS[m.group(2)]


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Thiếu biến môi trường {name}. Hãy tạo file .env từ .env.example")
    return value


@dataclass(frozen=True)
class Settings:
    mqtt_host: str
    mqtt_port: int
    mqtt_topic: str
    mqtt_qos: int
    influx_url: str
    influx_org: str
    influx_token: str
    bucket_raw: str
    bucket_processed: str
    raw_retention_s: int
    processed_retention_s: int
    stats_interval_s: int


def load_settings() -> Settings:
    return Settings(
        mqtt_host=_env("MQTT_HOST", "broker.hivemq.com"),
        mqtt_port=int(_env("MQTT_PORT", "1883")),
        mqtt_topic=_env("MQTT_TOPIC", "iot/ptit/+/+/telemetry"),
        mqtt_qos=int(_env("MQTT_QOS", "1")),
        influx_url=_env("INFLUX_URL", "http://localhost:8086"),
        influx_org=_env("INFLUX_ORG", "ptit"),
        influx_token=_env("INFLUX_TOKEN"),
        bucket_raw=_env("INFLUX_BUCKET_RAW", "iot_raw"),
        bucket_processed=_env("INFLUX_BUCKET_PROCESSED", "iot_processed"),
        raw_retention_s=parse_duration(_env("INFLUX_RAW_RETENTION", "7d")),
        processed_retention_s=parse_duration(_env("INFLUX_PROCESSED_RETENTION", "30d")),
        stats_interval_s=int(_env("STATS_INTERVAL_S", "30")),
    )
