"""Chạy tiền xử lý: đọc iot_raw -> pipeline -> ghi iot_processed + xuất báo cáo.

Ví dụ:
  python -m preprocess.run --start -1h                 # xử lý 1 giờ gần nhất, 1 lần
  python -m preprocess.run --start -2h --method zscore # dùng Z-score thay vì IQR
  python -m preprocess.run --start -30m --loop 60      # chạy liên tục mỗi 60 s (cho dashboard)
  python -m preprocess.run --csv data.csv --no-write   # chạy offline từ file CSV
Kết quả: bucket iot_processed (measurement sensor_processed) và thư mục reports/.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import time
from pathlib import Path

import pandas as pd

from common.config import ROOT
from preprocess.pipeline import compare_methods, run_pipeline

log = logging.getLogger("preprocess")
REPORT_DIR = ROOT / "reports"
INT_COLS = {"n_samples", "filled"}


def split_by_device(raw: pd.DataFrame) -> dict[str, tuple[str, pd.DataFrame]]:
    """{device_id: (location, DataFrame index thời gian)}"""
    out = {}
    if "device_id" not in raw:
        raw = raw.assign(device_id="unknown")
    for dev, g in raw.groupby("device_id"):
        loc = str(g["location"].iloc[-1]) if "location" in g else "unknown"
        out[str(dev)] = (loc, g.set_index("_time").sort_index())
    return out


def to_points(df: pd.DataFrame, device: str, location: str, method: str):
    from influxdb_client import Point, WritePrecision

    pts = []
    for ts, row in df.iterrows():
        p = Point("sensor_processed").tag("device_id", device).tag("location", location) \
            .tag("method", method).time(int(ts.value // 1_000_000), WritePrecision.MS)
        has_field = False
        for col, v in row.items():
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue        # InfluxDB không lưu NaN -> bỏ qua field này
            p = p.field(col, int(v) if col in INT_COLS else float(v))
            has_field = True
        if has_field:
            pts.append(p)
    return pts


def save_plot(raw: pd.DataFrame, cleaned: pd.DataFrame, out: pd.DataFrame, path: Path, title: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fields = [("temperature", "Nhiệt độ (°C)"), ("humidity", "Độ ẩm (%RH)"), ("light", "Ánh sáng (lux)")]
    fields = [f for f in fields if f[0] in raw]
    fig, axes = plt.subplots(len(fields), 1, figsize=(12.5, 3.2 * len(fields)), sharex=True)
    axes = [axes] if len(fields) == 1 else axes
    for ax, (col, label) in zip(axes, fields):
        ax.plot(raw.index, raw[col], ".", ms=2.5, color="#9aa0a6", label="Dữ liệu thô")
        if col in cleaned:
            outl = raw[col][cleaned[col].reindex(raw.index).isna() & raw[col].notna()]
            if len(outl):
                ax.plot(outl.index, outl.values, "x", color="#d93025", ms=7, mew=2,
                        label=f"Outlier ({len(outl)})")
        ax.plot(out.index, out[col], "-", color="#1a73e8", lw=1.8, label="Sau xử lý (resample)")
        if f"{col}_roll5" in out:
            ax.plot(out.index, out[f"{col}_roll5"], "--", color="#f29900", lw=1.4, label="Rolling mean")
        filled = out[out.get("filled", 0) == 1]
        if len(filled):
            ax.plot(filled.index, filled[col], "o", mfc="none", color="#188038", ms=6, label="Điểm nội suy")
        ax.set_ylabel(label)
        ax.grid(alpha=0.3)
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), fontsize=8, frameon=False)
    axes[0].set_title(title)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def print_report(device: str, rep: dict, cmp: pd.DataFrame) -> None:
    print(f"\n===== Thiết bị {device} =====")
    print(f"Bản ghi thô              : {rep['raw_rows']}")
    print(f"Bản ghi trùng đã bỏ      : {rep['duplicate_rows']}")
    print(f"Giá trị thiếu (thô)      : {rep['missing_raw']}")
    print(f"Outlier ({rep['outliers'].get('method')})       : "
          f"{ {k: v for k, v in rep['outliers'].items() if k != 'method'} }")
    print(f"Số cửa sổ resample       : {rep['bins_total']}  (trống {rep['bins_empty']}, "
          f"đã nội suy {rep['bins_filled']}, còn thiếu {rep['bins_still_missing']})")
    print("So sánh IQR và Z-score   :")
    print(cmp.to_string(index=False))


def process_once(args, client=None, settings=None) -> None:
    if args.csv:
        raw = pd.read_csv(args.csv)
        raw["_time"] = pd.to_datetime(raw["_time"], utc=True)
    else:
        from common.influx import query_raw
        raw = query_raw(client, settings, args.start, args.stop, args.device,
                        fields=["temperature", "humidity", "light", "motion"])
    if raw.empty:
        log.warning("Không có dữ liệu trong khoảng %s -> %s", args.start, args.stop)
        return

    REPORT_DIR.mkdir(exist_ok=True)
    for dev, (loc, df) in split_by_device(raw).items():
        cols = [c for c in ("temperature", "humidity", "light", "motion") if c in df]
        out, cleaned, rep = run_pipeline(df[cols], method=args.method, rule=args.rule,
                                         max_gap_bins=args.max_gap, roll=args.roll)
        cmp = compare_methods(df[cols][~df.index.duplicated(keep="last")].sort_index())
        rd = rep.as_dict()

        if not args.no_write and client is not None:
            pts = to_points(out, dev, loc, args.method)
            client.write_api().write(bucket=settings.bucket_processed, org=settings.influx_org, record=pts)
            log.info("Đã ghi %d điểm vào %s/sensor_processed", len(pts), settings.bucket_processed)

        if not args.quiet:
            print_report(dev, rd, cmp)
            safe = dev.replace("/", "_")
            df.to_csv(REPORT_DIR / f"{safe}_raw.csv")
            out.to_csv(REPORT_DIR / f"{safe}_processed.csv")
            cmp.to_csv(REPORT_DIR / f"{safe}_outlier_compare.csv", index=False)
            (REPORT_DIR / f"{safe}_report.json").write_text(json.dumps(rd, indent=2, ensure_ascii=False))
            save_plot(df, cleaned, out, REPORT_DIR / f"{safe}_preprocess.png",
                      f"{dev} – trước và sau tiền xử lý ({args.method}, {args.rule})")
            print(f"Đã lưu CSV, JSON và biểu đồ vào {REPORT_DIR}/")


def main() -> None:
    ap = argparse.ArgumentParser(description="Tiền xử lý dữ liệu IoT")
    ap.add_argument("--start", default="-1h", help="vd -1h, -30m hoặc 2026-10-18T02:00:00Z")
    ap.add_argument("--stop", default="now()")
    ap.add_argument("--device", default=None, help="chỉ xử lý một device_id")
    ap.add_argument("--method", choices=["iqr", "zscore"], default="iqr")
    ap.add_argument("--rule", default="1min", help="cửa sổ resample: 30s, 1min, 5min...")
    ap.add_argument("--max-gap", type=int, default=5, help="số cửa sổ trống tối đa được nội suy")
    ap.add_argument("--roll", type=int, default=5, help="độ dài cửa sổ rolling mean")
    ap.add_argument("--loop", type=int, default=0, help="chạy lặp mỗi N giây (0 = chạy 1 lần)")
    ap.add_argument("--csv", default=None, help="đọc dữ liệu thô từ CSV thay vì InfluxDB")
    ap.add_argument("--no-write", action="store_true", help="không ghi vào iot_processed")
    args = ap.parse_args()
    args.quiet = False

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    client = settings = None
    if not args.csv:
        from common.config import load_settings
        from common.influx import ensure_all_buckets, make_client
        settings = load_settings()
        client = make_client(settings)
        ensure_all_buckets(client, settings)

    try:
        if args.loop <= 0:
            process_once(args, client, settings)
            return
        log.info("Chạy lặp mỗi %ds trên cửa sổ %s. Ctrl+C để dừng.", args.loop, args.start)
        first = True
        while True:
            args.quiet = not first          # chỉ in báo cáo và vẽ biểu đồ ở lần đầu
            process_once(args, client, settings)
            first = False
            time.sleep(args.loop)
    except KeyboardInterrupt:
        pass
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()
