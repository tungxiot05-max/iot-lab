import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.latency_report import seq_loss  # noqa: E402
from preprocess.pipeline import _fill_short_gaps, compare_methods, run_pipeline  # noqa: E402


def make_raw(seed=1):
    """Dữ liệu giống Wokwi: mức không đổi, có bước nhảy, spike, mất dữ liệu và bản ghi trùng."""
    rng = np.random.default_rng(seed)
    n = 1800
    idx = pd.date_range("2026-10-18 02:00", periods=n, freq="2s", tz="UTC")
    df = pd.DataFrame({
        "temperature": np.where(np.arange(n) < 600, 25.0, 30.0) + rng.normal(0, .05, n),
        "humidity": np.full(n, 60.0) + rng.normal(0, .1, n),
        "light": np.full(n, 300.0),
        "motion": (rng.random(n) < .1).astype(int),
    }, index=idx)
    for i in (100, 350, 700, 1100):
        df.iloc[i, 0] += 35
    df.iloc[400, 1] = 99.0
    df = df.drop(df.index[1000:1090]).drop(df.index[1300:1600])
    return pd.concat([df, df.iloc[[5, 6]]])


def test_outliers_detected_and_step_not_flagged():
    out, cleaned, rep = run_pipeline(make_raw())
    assert rep.duplicate_rows == 2
    assert rep.outliers["temperature"] == 4          # 4 spike, không tính bước nhảy 25 -> 30
    assert rep.outliers["humidity"] == 1
    assert out["temperature"].max() < 31


def test_iqr_and_zscore_agree_on_clear_spikes():
    df = make_raw()
    df = df[~df.index.duplicated()].sort_index()
    cmp = compare_methods(df).set_index("field")
    assert cmp.loc["temperature", "both"] == 4


def test_resample_and_gap_policy():
    out, _, rep = run_pipeline(make_raw(), rule="1min", max_gap_bins=5)
    assert rep.bins_total == 60
    assert rep.bins_filled >= 1                      # khoảng mất 3 phút được nội suy
    assert rep.bins_still_missing >= 8               # khoảng mất 10 phút để trống
    assert out["temperature_roll5"].isna().sum() == out["temperature"].isna().sum()


def test_normalization_range():
    out, _, _ = run_pipeline(make_raw())
    v = out["temperature_norm"].dropna()
    assert v.min() >= 0 and v.max() <= 1


def test_fill_short_gaps_only():
    idx = pd.date_range("2026-10-18", periods=20, freq="1min", tz="UTC")
    s = pd.Series(np.arange(20, dtype=float), index=idx)
    s.iloc[3:5] = np.nan
    s.iloc[8:16] = np.nan
    r = _fill_short_gaps(s, 5)
    assert r.iloc[3] == 3 and r.iloc[8:16].isna().all()


def test_seq_loss_with_reboot():
    seq = pd.Series([1, 2, 3, 5, 6, 6, 9, 1, 2, 4])     # mất 4,7,8 ; reboot ; mất 3
    assert seq_loss(seq) == (13, 9)
