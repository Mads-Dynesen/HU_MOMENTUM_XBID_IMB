import io
import time
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
from pandas.core.interchange.dataframe_protocol import DataFrame
from pandas.io.parsers import TextFileReader

import PostgreSQLHandler
import sql_module


def _connect_dev() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        user="postgres",
        password="holtholt",
        host="development2.ctx8vvewoarr.eu-central-1.rds.amazonaws.com",
        database="postgres",
        application_name="DK1_model_DEM_DEV",
    )


def _read_sql_copy(query: str, conn: psycopg2.extensions.connection) -> TextFileReader | DataFrame:
    with conn.cursor() as cur:
        buf = io.StringIO()
        cur.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", buf)
        buf.seek(0)

    return pd.read_csv(buf, keep_default_na=False)


def fetch_data(sql_template: str, total_hours: int, name: str) -> tuple[pd.DataFrame, bool]:
    print(f"Fetching from DB: {name}")
    sql = sql_template.format(total_hours=total_hours)

    try:
        with _connect_dev() as conn:
            df = _read_sql_copy(sql, conn)
    except Exception as e:
        print(f"Error fetching {name} from DB: {e}")
        return pd.DataFrame(), False

    if "datetime_begin" in df.columns:
        df["datetime_begin"] = pd.to_datetime(
            df["datetime_begin"],
            utc=True,
            errors="coerce",
            format="mixed",
        )

    if "trade_exec_time" in df.columns:
        df["trade_exec_time"] = pd.to_datetime(
            df["trade_exec_time"],
            utc=True,
            errors="coerce",
            format="mixed",
        )

    return df, True


def add_grouped_ivwap(df: pd.DataFrame, window: int, output_col: str) -> pd.DataFrame:
    required_cols = ["price", "qt", "datetime_begin", "trade_exec_time"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in add_grouped_ivwap: {missing_cols}")

    df = df.copy()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qt"] = pd.to_numeric(df["qt"], errors="coerce")

    df = df.sort_values(["datetime_begin", "trade_exec_time"])

    result = pd.Series(index=df.index, dtype=float)
    tol = 1e-9

    for _, group in df.groupby("datetime_begin", sort=False):
        px = group["price"].to_numpy()
        mw = group["qt"].to_numpy()

        values: list[float] = []

        for i in range(len(group)):
            remaining = window
            weighted_sum = 0.0
            used = 0.0

            j = i
            while j >= 0 and remaining > 0:
                take = min(mw[j], remaining)
                weighted_sum += px[j] * take
                used += take
                remaining -= take
                j -= 1

            values.append(weighted_sum / used if used >= window - tol else np.nan)

        result.loc[group.index] = values

    df[output_col] = result
    return df


def add_grouped_ema(
    df: pd.DataFrame,
    span: int,
    output_col: str,
    price_col: str = "price",
) -> pd.DataFrame:
    required_cols = ["datetime_begin", "trade_exec_time", price_col]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in add_grouped_ema: {missing_cols}")

    df = df.copy()
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.sort_values(["datetime_begin", "trade_exec_time"])

    df[output_col] = (
        df.groupby("datetime_begin")[price_col]
        .transform(lambda s: s.ewm(span=span, adjust=False).mean())
    )

    return df


def first_signal(group: pd.DataFrame) -> Optional[pd.Series]:
    group = group.sort_values("trade_exec_time")
    matched = group[group["signal"]]
    return matched.iloc[0] if not matched.empty else None


def add_grouped_wap(
    df: pd.DataFrame,
    price_col: str = "price_at",
    qty_col: str = "qt_at",
    output_col: str = "wap_at",
) -> pd.DataFrame:
    df = df.copy()

    required_cols = ["datetime_begin", price_col, qty_col]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in add_grouped_wap: {missing_cols}")

    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce")

    grouped_wap = (
        df.groupby("datetime_begin", as_index=False)
        .apply(
            lambda g: pd.Series(
                {
                    output_col: (g[price_col] * g[qty_col]).sum() / g[qty_col].sum()
                    if g[qty_col].sum() != 0
                    else np.nan
                }
            )
        )
        .reset_index(drop=True)
    )

    return grouped_wap


def create_signal(
    total_hours: int = 0,
    short_window: int = 5,
    long_window: int = 20,
    ema_short_span: int = 5,
    ema_long_span: int = 20,
    threshold: float = 5,
    target_delivery: Optional[pd.Timestamp] = None,
) -> tuple[pd.DataFrame, bool]:
    df_trade, ok_trade = fetch_data(sql_module.sql_trade, total_hours, "trade")
    df_spot, ok_spot = fetch_data(sql_module.sql_spot_prices, total_hours, "spot")
    df_consumption, ok_consumption = fetch_data(sql_module.sql_consumption, total_hours, "consumption")

    if not (ok_trade and ok_spot and ok_consumption):
        return pd.DataFrame(), False

    if df_trade.empty or df_spot.empty or df_consumption.empty:
        return pd.DataFrame(), False

    if target_delivery is not None:
        df_trade = df_trade[df_trade["datetime_begin"] == target_delivery]
        df_spot = df_spot[df_spot["datetime_begin"] == target_delivery]
        df_consumption = df_consumption[df_consumption["datetime_begin"] == target_delivery]

        if df_trade.empty:
            return pd.DataFrame(), False

    df_trade = add_grouped_ivwap(df_trade, short_window, "ivwap_5mw")
    df_trade = add_grouped_ivwap(df_trade, long_window, "ivwap_20mw")

    df_trade = add_grouped_ema(df_trade, ema_short_span, "ema_short")
    df_trade = add_grouped_ema(df_trade, ema_long_span, "ema_long")

    df = df_trade.merge(df_spot, on="datetime_begin", how="left")
    df["spot"] = pd.to_numeric(df["spot"], errors="coerce")

    df["signal"] = (
        (df["ivwap_5mw"] > df["ivwap_20mw"] + threshold)
        & (df["ivwap_5mw"] > df["spot"])
        & (df["ema_short"] > df["ema_long"])
    )

    df_signals = (
        df.groupby("datetime_begin", group_keys=True)
        .apply(first_signal)
        .dropna()
        .reset_index(drop=True)
    )

    if df_signals.empty:
        return pd.DataFrame(), False

    df_signals = df_signals.merge(df_consumption, on="datetime_begin", how="left")
    df_signals["forecasted_delta"] = pd.to_numeric(
        df_signals["forecasted_delta"], errors="coerce"
    )

    result = df_signals[df_signals["forecasted_delta"] < -300]

    return result, not result.empty


def run_model(poll_seconds: int = 30) -> tuple[pd.DataFrame, bool]:
    target_delivery = (pd.Timestamp.now(tz="utc") + timedelta(hours=4)).floor("h")
    print(f"Tracking delivery hour: {target_delivery}")

    deadline = (pd.Timestamp.now(tz="utc") + timedelta(hours=2)).floor("h")

    while pd.Timestamp.now(tz="utc") < deadline:
        try:
            data, ok = create_signal(target_delivery=target_delivery)

            if ok:
                return data, True

            print(f"No signal yet for {target_delivery}")

        except Exception as e:
            print(f"Error in polling loop for {target_delivery}: {e}")

        time.sleep(poll_seconds)

    print(f"No signal found for {target_delivery} within 3 hours")
    return pd.DataFrame(), False



def make_dataframe_for_db_and_insert() -> bool:
    df, ok = run_model()

    if df.empty or not ok:
        return False

    df = df.copy()
    df["price"] = df["ivwap_5mw"] + 20
    df = df.rename(columns={"signal": "REGULATION_STATE"})
    df["REGULATION_STATE"] = df["REGULATION_STATE"].astype(int)

    df = pd.melt(
        df,
        id_vars=["datetime_begin"],
        value_vars=["price", "REGULATION_STATE"],
        var_name="value_type",
        value_name="fact_value",
    )

    df["dim_data_provider_sk"] = "Inductive Energy"
    df["dim_power_price_area_sk"] = "MAVIR"
    df["dim_granularity_sk"] = "60_MIN"
    df["dim_strategy_sk"] = "HU_MOMENTUM_XBID_IMB"
    df["dim_product_type_sk"] = "XBID"
    df["dim_signal_type_sk"] = np.where(
        df["value_type"] == "REGULATION_STATE",
        "DIRECTION",
        "LIMIT_PRICE",
    )
    df["dim_direction_sk"] = "BUY"

    try:
        PostgreSQLHandler.insert_pandas_into_db(df, "fact_signal")
        return True
    except Exception as e:
        print(f"Error inserting into fact_signal: {e}")
        return False

if __name__ == "__main__":
    ok = make_dataframe_for_db_and_insert()
