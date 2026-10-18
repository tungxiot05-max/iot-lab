"""Sinh file dashboard Grafana (grafana/provisioning/dashboards/json/iot_lab2.json).
Chạy lại script này nếu đổi tên bucket:  python tools/build_dashboard.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = os.getenv("INFLUX_BUCKET_RAW", "iot_raw")
PROC = os.getenv("INFLUX_BUCKET_PROCESSED", "iot_processed")
DS = {"type": "influxdb", "uid": "influxdb"}
RANGE = "range(start: v.timeRangeStart, stop: v.timeRangeStop)"


def q_field(bucket: str, meas: str, field: str, label: str, last: bool = False, extra: str = "") -> str:
    """Truy vấn một field, đặt tên series = label. Dashboard giả định một thiết bị
    (nhiều thiết bị sẽ bị gộp chung một đường)."""
    return (f'from(bucket: "{bucket}")\n  |> {RANGE}\n'
            f'  |> filter(fn: (r) => r._measurement == "{meas}" and r._field == "{field}"){extra}\n'
            f'  |> keep(columns: ["_time", "_value"])\n'
            f'  |> set(key: "_field", value: "{label}")\n'
            f'  |> group(columns: ["_field"])\n'
            f'  |> sort(columns: ["_time"])'
            + ("\n  |> last()" if last else ""))


def target(ref: str, query: str) -> dict:
    return {"refId": ref, "datasource": DS, "query": query}


_id = 0


def panel(title, typ, x, y, w, h, targets, unit="", decimals=None, extra=None, desc=""):
    global _id
    _id += 1
    p = {"id": _id, "type": typ, "title": title, "description": desc, "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [target(chr(65 + i), q) for i, q in enumerate(targets)],
         "fieldConfig": {"defaults": {"unit": unit}, "overrides": []}, "options": {}}
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    if typ == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 2, "pointSize": 4, "showPoints": "auto",
                                                  "spanNulls": False}
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"},
                        "tooltip": {"mode": "multi"}}
    if typ == "stat":
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "values": False},
                        "colorMode": "value", "graphMode": "area", "textMode": "value_and_name"}
    if extra:
        for k, v in extra.items():
            p.setdefault(k, {}).update(v) if isinstance(v, dict) else p.__setitem__(k, v)
    return p


def row(title, y):
    global _id
    _id += 1
    return {"id": _id, "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def overrides(names_colors: dict[str, str], dashed: tuple[str, ...] = (), points_only: tuple[str, ...] = ()):
    ov = []
    for name, color in names_colors.items():
        props = [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]
        if name in dashed:
            props.append({"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [8, 6]}})
        if name in points_only:
            props += [{"id": "custom.drawStyle", "value": "points"}, {"id": "custom.pointSize", "value": 3}]
        ov.append({"matcher": {"id": "byName", "options": name}, "properties": props})
    return ov


def build() -> dict:
    P = []
    y = 0
    # ---------------- Hàng 1: real-time
    P.append(row("Real-time – dữ liệu thô từ thiết bị", y)); y += 1
    for i, (f, lbl, unit) in enumerate([("temperature", "Nhiệt độ", "celsius"), ("humidity", "Độ ẩm", "humidity"),
                                        ("light", "Ánh sáng", "lux"), ("rssi", "RSSI", "dBm")]):
        P.append(panel(lbl, "stat", i * 6, y, 6, 4, [q_field(RAW, "sensor_raw", f, lbl)], unit,
                       1 if f != "rssi" else 0))
    y += 4
    P.append(panel("Nhiệt độ & độ ẩm (thô)", "timeseries", 0, y, 12, 8,
                   [q_field(RAW, "sensor_raw", "temperature", "Nhiệt độ (°C)"),
                    q_field(RAW, "sensor_raw", "humidity", "Độ ẩm (%RH)")], "", 1))
    P.append(panel("Ánh sáng & chuyển động (thô)", "timeseries", 12, y, 12, 8,
                   [q_field(RAW, "sensor_raw", "light", "Ánh sáng (lux)"),
                    q_field(RAW, "sensor_raw", "motion", "Chuyển động (0/1)")], "", 1,
                   extra={"fieldConfig": {"overrides": [
                       {"matcher": {"id": "byName", "options": "Chuyển động (0/1)"},
                        "properties": [{"id": "custom.axisPlacement", "value": "right"},
                                       {"id": "custom.drawStyle", "value": "bars"}]}]}}))
    y += 8

    # ---------------- Hàng 2: sau tiền xử lý
    P.append(row("Sau tiền xử lý – so sánh với dữ liệu thô", y)); y += 1
    for i, (f, lbl, unit) in enumerate([("temperature", "Nhiệt độ", "celsius"), ("humidity", "Độ ẩm", "humidity")]):
        raw_name, proc_name, roll_name = f"{lbl} thô", f"{lbl} sau xử lý", f"{lbl} rolling mean"
        P.append(panel(f"{lbl}: thô vs sau xử lý", "timeseries", i * 12, y, 12, 9,
                       [q_field(RAW, "sensor_raw", f, raw_name),
                        q_field(PROC, "sensor_processed", f, proc_name),
                        q_field(PROC, "sensor_processed", f"{f}_roll5", roll_name)], unit, 1,
                       desc="Chấm xám: dữ liệu thô (có outlier). Xanh: đã loại outlier, resample, nội suy. "
                            "Cam đứt: rolling mean 5 cửa sổ.",
                       extra={"fieldConfig": {"overrides": overrides(
                           {raw_name: "#9aa0a6", proc_name: "#1a73e8", roll_name: "#f29900"},
                           dashed=(roll_name,), points_only=(raw_name,))}}))
    y += 9
    P.append(panel("Dữ liệu chuẩn hoá Min-Max [0, 1]", "timeseries", 0, y, 12, 8,
                   [q_field(PROC, "sensor_processed", "temperature_norm", "Nhiệt độ"),
                    q_field(PROC, "sensor_processed", "humidity_norm", "Độ ẩm"),
                    q_field(PROC, "sensor_processed", "light_norm", "Ánh sáng")], "", 2))
    P.append(panel("Delta – tốc độ thay đổi mỗi cửa sổ", "timeseries", 12, y, 12, 8,
                   [q_field(PROC, "sensor_processed", "temperature_delta", "Δ Nhiệt độ"),
                    q_field(PROC, "sensor_processed", "humidity_delta", "Δ Độ ẩm")], "", 2))
    y += 8

    # ---------------- Hàng 3: sức khoẻ pipeline
    P.append(row("Sức khoẻ pipeline – độ trễ, mất gói", y)); y += 1
    stat = lambda f, lbl: q_field(RAW, "pipeline_stats", f, lbl)  # noqa: E731
    for i, (f, lbl, unit, dec) in enumerate([("loss_pct", "Tỷ lệ mất gói", "percent", 2),
                                             ("lat_p95_ms", "Độ trễ p95", "ms", 0),
                                             ("received", "Gói đã nhận", "none", 0),
                                             ("invalid", "Gói bị loại", "none", 0),
                                             ("lost", "Gói bị mất", "none", 0),
                                             ("duplicate", "Gói trùng", "none", 0)]):
        P.append(panel(lbl, "stat", i * 4, y, 4, 4, [stat(f, lbl)], unit, dec))
    y += 4
    P.append(panel("Độ trễ end-to-end từng gói", "timeseries", 0, y, 12, 8,
                   [q_field(RAW, "sensor_raw", "latency_ms", "Độ trễ từng gói")], "ms", 0,
                   extra={"fieldConfig": {"overrides": overrides({"Độ trễ từng gói": "#1a73e8"},
                                                                 points_only=("Độ trễ từng gói",))}}))
    P.append(panel("Độ trễ theo chu kỳ thống kê (30 s)", "timeseries", 12, y, 12, 8,
                   [stat("lat_avg_ms", "Trung bình"), stat("lat_p95_ms", "p95"), stat("lat_max_ms", "Max"),
                    stat("write_avg_ms", "Thời gian ghi DB")], "ms", 1))
    y += 8
    P.append(panel("Độ lệch đồng hồ thiết bị so với server", "timeseries", 0, y, 24, 7,
                   [q_field(RAW, "sensor_raw", "clock_offset_ms", "Giờ server − giờ thiết bị")], "ms", 0,
                   desc="Lệch quá MAX_CLOCK_SKEW_MS thì gói được lưu theo giờ server và không tính độ trễ. "
                        "Wokwi chạy chậm hơn thời gian thực làm đường này tăng dần; firmware đồng bộ NTP lại mỗi 5 phút."))
    return {
        "uid": "iot-lab2", "title": "IoT Lab 2 – Giám sát cảm biến", "tags": ["iot", "lab2"],
        "timezone": "browser", "editable": True, "refresh": "5s", "schemaVersion": 39,
        "time": {"from": "now-30m", "to": "now"},
        "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
        "panels": P,
    }


if __name__ == "__main__":
    out = ROOT / "grafana" / "provisioning" / "dashboards" / "json" / "iot_lab2.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(build(), indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Đã ghi {out} ({len(build()['panels'])} panel)")
