"""Kiểm tra tính hợp lệ của gói telemetry và theo dõi số thứ tự (seq).

Phân tầng trách nhiệm:
- Validator chỉ loại dữ liệu SAI về cấu trúc hoặc VÔ LÝ về vật lý (ngoài dải đo của cảm biến).
- Giá trị nằm trong dải đo nhưng bất thường về thống kê (spike) vẫn được lưu vào dữ liệu thô
  và để bước tiền xử lý (IQR / Z-score) phát hiện.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass

NUM = (int, float)

# Trường bắt buộc và kiểu
REQUIRED: dict[str, tuple] = {
    "device_id": (str,),
    "seq": (int,),
    "ts": (int,),
    "temperature": NUM,
    "humidity": NUM,
}

# Trường tuỳ chọn và kiểu
OPTIONAL: dict[str, tuple] = {
    "location": (str,),
    "light": NUM,
    "light_raw": (int,),
    "motion": (int,),
    "rssi": (int,),
    "uptime_s": (int,),
}

# Dải hợp lệ theo thông số cảm biến
RANGES: dict[str, tuple[float, float]] = {
    "temperature": (-40.0, 80.0),     # DHT22
    "humidity": (0.0, 100.0),         # DHT22
    "light": (0.0, 200_000.0),        # lux
    "light_raw": (0, 4095),           # ADC 12 bit
    "motion": (0, 1),                 # PIR
    "rssi": (-120, 0),                # dBm
    "seq": (0, 2**32 - 1),
    "uptime_s": (0, 10**9),
}

FLOAT_FIELDS = ("temperature", "humidity", "light")
INT_FIELDS = ("seq", "ts", "light_raw", "motion", "rssi", "uptime_s")

MIN_VALID_TS_MS = 1_700_000_000_000   # 11/2023: nhỏ hơn => thiết bị chưa đồng bộ NTP


class ValidationError(ValueError):
    pass


def _type_ok(value, types: tuple) -> bool:
    if isinstance(value, bool):          # bool là lớp con của int -> loại riêng
        return False
    if types == (int,) and isinstance(value, float) and value.is_integer():
        return True
    return isinstance(value, types)


def validate(raw: bytes | str, topic: str, now_ms: int) -> dict:
    """Trả về bản ghi đã chuẩn hoá kiểu, hoặc ném ValidationError kèm lý do."""
    try:
        data = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValidationError(f"JSON không hợp lệ: {e}") from None
    if not isinstance(data, dict):
        raise ValidationError("Payload phải là đối tượng JSON")

    for key, types in REQUIRED.items():
        if key not in data or data[key] is None:
            raise ValidationError(f"Thiếu trường bắt buộc '{key}'")
        if not _type_ok(data[key], types):
            raise ValidationError(f"Sai kiểu '{key}': {type(data[key]).__name__}")
    for key, types in OPTIONAL.items():
        if key in data and data[key] is not None and not _type_ok(data[key], types):
            raise ValidationError(f"Sai kiểu '{key}': {type(data[key]).__name__}")

    rec: dict = {"device_id": data["device_id"].strip(), "location": str(data.get("location") or "unknown")}
    if not rec["device_id"]:
        raise ValidationError("device_id rỗng")

    for key in FLOAT_FIELDS:
        if data.get(key) is not None:
            v = float(data[key])
            if not math.isfinite(v):
                raise ValidationError(f"'{key}' là NaN/Inf")
            rec[key] = v
    for key in INT_FIELDS:
        if data.get(key) is not None:
            rec[key] = int(data[key])

    for key, (lo, hi) in RANGES.items():
        if key in rec and not (lo <= rec[key] <= hi):
            raise ValidationError(f"'{key}'={rec[key]} ngoài dải [{lo}, {hi}]")

    if rec["ts"] < MIN_VALID_TS_MS:
        raise ValidationError("ts quá nhỏ: thiết bị chưa đồng bộ NTP")
    # Đồng hồ thiết bị lệch (nhanh hoặc chậm) không làm gói bị loại: ingest sẽ lưu theo giờ server.

    # Topic chuẩn: iot/<site>/<location>/<device_id>/telemetry
    parts = topic.split("/")
    if len(parts) == 5 and parts[0] == "iot" and parts[4] == "telemetry":
        if parts[3] != rec["device_id"]:
            raise ValidationError(f"device_id '{rec['device_id']}' không khớp topic '{topic}'")
        rec["site"] = parts[1]
    else:
        rec["site"] = "unknown"
    return rec


REBOOT_MAX_SEQ = 3
REBOOT_MAX_UPTIME_S = 30


@dataclass
class SeqResult:
    status: str   # first | ok | gap | duplicate | out_of_order | reboot
    lost: int = 0


class SeqTracker:
    """Phát hiện mất gói, trùng gói, gói đến muộn và thiết bị khởi động lại dựa trên seq + uptime_s."""

    def __init__(self) -> None:
        self._last: dict[str, tuple[int, int | None]] = {}

    def check(self, device_id: str, seq: int, uptime_s: int | None = None) -> SeqResult:
        prev = self._last.get(device_id)
        if prev is None:
            self._last[device_id] = (seq, uptime_s)
            return SeqResult("first")
        prev_seq, prev_up = prev
        if seq == prev_seq:
            return SeqResult("duplicate")
        if seq < prev_seq:
            # Gói đến muộn cũng có seq và uptime nhỏ hơn gói trước, nên chỉ coi là khởi động lại
            # khi bộ đếm quay về gần 0 (firmware đặt lại seq=1, uptime=0 sau reset).
            rebooted = seq <= REBOOT_MAX_SEQ or (uptime_s is not None and uptime_s <= REBOOT_MAX_UPTIME_S)
            if rebooted:
                self._last[device_id] = (seq, uptime_s)
                return SeqResult("reboot")
            return SeqResult("out_of_order")
        gap = seq - prev_seq - 1
        self._last[device_id] = (seq, uptime_s)
        return SeqResult("gap", gap) if gap > 0 else SeqResult("ok")
