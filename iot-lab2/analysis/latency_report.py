"""Thống kê độ trễ end-to-end và tỷ lệ mất gói từ dữ liệu đã lưu, xuất bảng + biểu đồ cho báo cáo.

  python -m analysis.latency_report --start -1h
  python -m analysis.latency_report --csv reports/<device>_latency.csv   (offline)

Độ trễ (latency_ms) = thời điểm ingest nhận gói - thời điểm thiết bị đo (ts, đồng bộ NTP).
Mất gói ước lượng từ seq: (seq_max - seq_min + 1) - số gói đã lưu, tính riêng từng đoạn
giữa các lần thiết bị khởi động lại (seq quay về nhỏ).
"""
from __future__ import annotations

import argparse
import json

import numpy as np
import pandas as pd

from common.config import ROOT

REPORT_DIR = ROOT / "reports"


def seq_loss(seq: pd.Series) -> tuple[int, int]:
    """Trả về (số gói kỳ vọng, số gói thực nhận), chia đoạn khi seq giảm (reboot)."""
    seq = seq.dropna().astype(int).reset_index(drop=True)
    if seq.empty:
        return 0, 0
    segment = (seq.diff() < 0).cumsum()
    expected = received = 0
    for _, s in seq.groupby(segment):
        u = s.drop_duplicates()
        expected += int(u.max() - u.min() + 1)
        received += len(u)
    return expected, received


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for dev, g in df.groupby("device_id"):
        lat = g["latency_ms"].dropna()
        exp, rec = seq_loss(g.sort_values("_time")["seq"]) if "seq" in g else (0, len(g))
        span = (g["_time"].max() - g["_time"].min()).total_seconds()
        rows.append({
            "device_id": dev, "packets": len(g), "duration_min": round(span / 60, 1),
            "rate_msg_per_s": round(len(g) / span, 3) if span > 0 else 0,
            "lat_min_ms": round(lat.min(), 1), "lat_mean_ms": round(lat.mean(), 1),
            "lat_p50_ms": round(lat.quantile(0.5), 1), "lat_p95_ms": round(lat.quantile(0.95), 1),
            "lat_p99_ms": round(lat.quantile(0.99), 1), "lat_max_ms": round(lat.max(), 1),
            "lat_std_ms": round(lat.std(), 1),
            "expected_by_seq": exp, "lost": exp - rec,
            "loss_pct": round(100 * (exp - rec) / exp, 2) if exp else 0.0,
        })
    return pd.DataFrame(rows)


def plot(df: pd.DataFrame, path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 3.8))
    for dev, g in df.groupby("device_id"):
        a1.plot(g["_time"], g["latency_ms"], ".", ms=3, label=dev)
        a2.hist(g["latency_ms"].dropna(), bins=40, alpha=0.7, label=dev)
    lat = df["latency_ms"].dropna()
    for q, ls in ((0.5, "-"), (0.95, "--")):
        a2.axvline(lat.quantile(q), color="k", ls=ls, lw=1, label=f"p{int(q * 100)} = {lat.quantile(q):.0f} ms")
    a1.set_title("Độ trễ end-to-end theo thời gian")
    a1.set_ylabel("ms")
    a1.grid(alpha=0.3)
    a2.set_title("Phân bố độ trễ")
    a2.set_xlabel("ms")
    a2.legend(fontsize=8)
    a1.legend(fontsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="-1h")
    ap.add_argument("--stop", default="now()")
    ap.add_argument("--device", default=None)
    ap.add_argument("--csv", default=None)
    args = ap.parse_args()

    if args.csv:
        df = pd.read_csv(args.csv)
        df["_time"] = pd.to_datetime(df["_time"], utc=True)
    else:
        from common.config import load_settings
        from common.influx import make_client, query_raw
        s = load_settings()
        with make_client(s) as client:
            df = query_raw(client, s, args.start, args.stop, args.device, fields=["latency_ms", "seq"])
    if df.empty:
        print("Không có dữ liệu.")
        return

    REPORT_DIR.mkdir(exist_ok=True)
    table = summarize(df)
    print(table.T.to_string(header=False))
    table.to_csv(REPORT_DIR / "latency_summary.csv", index=False)
    df.to_csv(REPORT_DIR / "latency_raw.csv", index=False)
    plot(df, REPORT_DIR / "latency.png")
    (REPORT_DIR / "latency_summary.json").write_text(
        json.dumps(table.to_dict(orient="records"), indent=2, default=lambda o: o.item()
                   if isinstance(o, np.generic) else str(o)))
    print(f"\nĐã lưu latency_summary.csv và latency.png vào {REPORT_DIR}/")


if __name__ == "__main__":
    main()
