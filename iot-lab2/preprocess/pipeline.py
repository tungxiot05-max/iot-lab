"""Các bước tiền xử lý dữ liệu cảm biến (chỉ dùng pandas/numpy/scikit-learn, không phụ thuộc InfluxDB).

Thứ tự: làm sạch -> phát hiện outlier -> resampling -> điền giá trị thiếu -> tạo đặc trưng -> chuẩn hoá.
Mỗi hàm nhận và trả DataFrame có index thời gian (UTC) của MỘT thiết bị.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler

# Các đại lượng được kiểm tra outlier và mức chênh lệch tối thiểu để coi là bất thường.
# Dữ liệu Wokwi gần như không nhiễu nên độ lệch chuẩn ~0; ngưỡng tối thiểu tránh việc
# mọi thay đổi nhỏ đều bị coi là outlier.
OUTLIER_FIELDS: dict[str, float] = {"temperature": 2.0, "humidity": 5.0}
SENSOR_FIELDS = ["temperature", "humidity", "light"]


@dataclass
class Report:
    """Thống kê từng bước, dùng cho bảng số liệu trong báo cáo."""
    raw_rows: int = 0
    duplicate_rows: int = 0
    rows_after_clean: int = 0
    missing_raw: dict = field(default_factory=dict)
    outliers: dict = field(default_factory=dict)
    bins_total: int = 0
    bins_empty: int = 0
    bins_filled: int = 0
    bins_still_missing: int = 0
    scaler_params: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return self.__dict__.copy()


# ---------------------------------------------------------------- 1. Làm sạch
def clean(df: pd.DataFrame, rep: Report) -> pd.DataFrame:
    """Sắp xếp theo thời gian, bỏ bản ghi trùng thời điểm, ép kiểu số."""
    rep.raw_rows = len(df)
    df = df.sort_index()
    dup = df.index.duplicated(keep="last")
    rep.duplicate_rows = int(dup.sum())
    df = df[~dup].copy()
    for c in SENSOR_FIELDS + ["motion"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    rep.missing_raw = {c: int(df[c].isna().sum()) for c in SENSOR_FIELDS if c in df}
    rep.rows_after_clean = len(df)
    return df


# ---------------------------------------------------------------- 2. Outlier
def _deviation(s: pd.Series, window: int) -> pd.Series:
    """Độ lệch so với trung vị trượt. Trung vị không bị kéo bởi một điểm spike,
    nên spike nổi bật, còn khi người dùng đổi mức nhiệt trên Wokwi (bước nhảy)
    thì trung vị theo kịp sau vài mẫu và không bị coi là bất thường."""
    med = s.rolling(window, center=True, min_periods=1).median()
    return s - med


def detect_outliers(s: pd.Series, method: str = "iqr", window: int = 5,
                    min_abs: float = 0.0, iqr_k: float = 1.5, z_thresh: float = 3.0) -> pd.Series:
    """Trả về Series bool: True = outlier.
    - iqr : độ lệch nằm ngoài [Q1 - k*IQR, Q3 + k*IQR]
    - zscore: |độ lệch - trung bình| / độ lệch chuẩn > z_thresh
    Cả hai còn yêu cầu |độ lệch| > min_abs (ý nghĩa vật lý)."""
    dev = _deviation(s, window)
    valid = dev.dropna()
    if valid.empty:
        return pd.Series(False, index=s.index)
    if method == "iqr":
        q1, q3 = valid.quantile(0.25), valid.quantile(0.75)
        iqr = q3 - q1
        stat = (dev < q1 - iqr_k * iqr) | (dev > q3 + iqr_k * iqr)
    elif method == "zscore":
        std = valid.std(ddof=0)
        stat = ((dev - valid.mean()).abs() / std > z_thresh) if std > 0 else pd.Series(False, index=s.index)
    else:
        raise ValueError("method phải là 'iqr' hoặc 'zscore'")
    return (stat & (dev.abs() > min_abs)).fillna(False)


def remove_outliers(df: pd.DataFrame, rep: Report, method: str = "iqr") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Thay outlier bằng NaN (sẽ được nội suy ở bước sau). Trả về (df đã xử lý, mask outlier)."""
    df = df.copy()
    mask = pd.DataFrame(False, index=df.index, columns=list(OUTLIER_FIELDS))
    for col, min_abs in OUTLIER_FIELDS.items():
        if col not in df:
            continue
        m = detect_outliers(df[col], method=method, min_abs=min_abs)
        mask[col] = m
        df.loc[m, col] = np.nan
    rep.outliers = {c: int(mask[c].sum()) for c in mask}
    rep.outliers["method"] = method
    return df, mask


def compare_methods(df: pd.DataFrame) -> pd.DataFrame:
    """Bảng so sánh số outlier IQR và Z-score cho từng đại lượng."""
    rows = []
    for col, min_abs in OUTLIER_FIELDS.items():
        if col not in df:
            continue
        iqr = detect_outliers(df[col], "iqr", min_abs=min_abs)
        z = detect_outliers(df[col], "zscore", min_abs=min_abs)
        rows.append({"field": col, "iqr": int(iqr.sum()), "zscore": int(z.sum()),
                     "both": int((iqr & z).sum()), "total_points": int(df[col].notna().sum())})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- 3+4. Resample & điền thiếu
def _fill_short_gaps(s: pd.Series, max_gap: int) -> pd.Series:
    """Nội suy tuyến tính theo thời gian, nhưng CHỈ cho các đoạn trống dài <= max_gap.
    Đoạn trống dài hơn giữ nguyên NaN toàn bộ (pandas `limit` sẽ điền một phần, không phù hợp)."""
    na = s.isna()
    run_id = (na != na.shift()).cumsum()
    run_len = na.groupby(run_id).transform("sum")
    fillable = na & (run_len <= max_gap)
    filled = s.interpolate(method="time", limit_area="inside")
    return s.where(~fillable, filled)


def resample_fill(df: pd.DataFrame, rep: Report, rule: str = "1min", max_gap_bins: int = 5) -> pd.DataFrame:
    """Gom dữ liệu theo cửa sổ thời gian `rule` (trung bình), rồi nội suy theo thời gian
    cho khoảng trống ngắn (<= max_gap_bins cửa sổ). Khoảng trống dài để NaN vì nội suy
    qua quãng mất dữ liệu dài sẽ tạo ra số liệu giả."""
    agg = {c: "mean" for c in SENSOR_FIELDS if c in df}
    if "motion" in df:
        agg["motion"] = "mean"       # tỷ lệ thời gian có chuyển động trong cửa sổ
    out = df.resample(rule).agg(agg)
    first = SENSOR_FIELDS[0]
    out["n_samples"] = df[first].resample(rule).count() if first in df else 0
    if "motion" in out:
        out = out.rename(columns={"motion": "motion_ratio"})

    empty = out[[c for c in SENSOR_FIELDS if c in out]].isna().any(axis=1)
    rep.bins_total = len(out)
    rep.bins_empty = int(empty.sum())

    for c in [c for c in SENSOR_FIELDS + ["motion_ratio"] if c in out]:
        out[c] = _fill_short_gaps(out[c], max_gap_bins)

    still = out[[c for c in SENSOR_FIELDS if c in out]].isna().any(axis=1)
    out["filled"] = (empty & ~still).astype(int)
    rep.bins_filled = int(out["filled"].sum())
    rep.bins_still_missing = int(still.sum())
    return out


# ---------------------------------------------------------------- 5. Đặc trưng
def add_features(df: pd.DataFrame, roll: int = 5) -> pd.DataFrame:
    """rolling mean (xu hướng) và delta (tốc độ thay đổi giữa hai cửa sổ liên tiếp)."""
    df = df.copy()
    for c in SENSOR_FIELDS:
        if c in df:
            # Không tạo giá trị ở cửa sổ đang thiếu dữ liệu (khoảng mất gói dài)
            df[f"{c}_roll{roll}"] = df[c].rolling(roll, min_periods=1).mean().where(df[c].notna())
            df[f"{c}_delta"] = df[c].diff()
    return df


# ---------------------------------------------------------------- 6. Chuẩn hoá
def normalize(df: pd.DataFrame, rep: Report) -> pd.DataFrame:
    """Min-Max về [0, 1] (_norm) và Z-score (_z) cho từng cảm biến. Lưu tham số để tái lập."""
    df = df.copy()
    for c in SENSOR_FIELDS:
        if c not in df:
            continue
        col = df[[c]]
        ok = col[c].notna()
        df[f"{c}_norm"] = np.nan
        df[f"{c}_z"] = np.nan
        if ok.sum() < 2:
            continue
        mm = MinMaxScaler().fit(col[ok])
        st = StandardScaler().fit(col[ok])
        df.loc[ok, f"{c}_norm"] = mm.transform(col[ok]).ravel()
        df.loc[ok, f"{c}_z"] = st.transform(col[ok]).ravel()
        if mm.data_range_[0] == 0:          # dữ liệu không đổi -> tránh hiểu nhầm
            df.loc[ok, f"{c}_norm"] = 0.0
        rep.scaler_params[c] = {"min": float(mm.data_min_[0]), "max": float(mm.data_max_[0]),
                                "mean": float(st.mean_[0]), "std": float(np.sqrt(st.var_[0]))}
    return df


# ---------------------------------------------------------------- Toàn bộ pipeline
def run_pipeline(raw: pd.DataFrame, method: str = "iqr", rule: str = "1min",
                 max_gap_bins: int = 5, roll: int = 5) -> tuple[pd.DataFrame, pd.DataFrame, Report]:
    """raw: DataFrame index thời gian, cột temperature/humidity/light/motion của MỘT thiết bị.
    Trả về (processed, cleaned_with_outliers_removed, report)."""
    rep = Report()
    df = clean(raw, rep)
    df_no_out, _ = remove_outliers(df, rep, method)
    out = resample_fill(df_no_out, rep, rule, max_gap_bins)
    out = add_features(out, roll)
    out = normalize(out, rep)
    return out, df_no_out, rep
