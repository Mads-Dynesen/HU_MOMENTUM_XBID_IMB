import io
import numpy as np
import pandas as pd
import psycopg2

import src.sql_module as sql_module





def _connect_dev():
    return psycopg2.connect(
        user="postgres",
        password="holtholt",
        host="development2.ctx8vvewoarr.eu-central-1.rds.amazonaws.com",
        database="postgres",
        application_name="DK1_model_DEM_DEV",
    )


def _read_sql_copy(query: str, conn):
    with conn.cursor() as cur:
        buf = io.StringIO()
        cur.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", buf)
        buf.seek(0)
    return pd.read_csv(buf, keep_default_na=False)




def fetch_data(sql_template, total_hours, name):

    print(f"Fetching from DB: {name}")
    sql = sql_template.format(total_hours=total_hours)

    with _connect_dev() as conn:
        df = _read_sql_copy(sql, conn)



    # parse datetime
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

    return df


def add_grouped_ivwap(df, window):
    df = df.copy()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qt"] = pd.to_numeric(df["qt"], errors="coerce")

    df = df.sort_values(["datetime_begin", "trade_exec_time"])

    result = pd.Series(index=df.index, dtype=float)
    tol = 1e-9

    for _, group in df.groupby("datetime_begin", sort=False):
        px = group["price"].to_numpy()
        mw = group["qt"].to_numpy()

        values = []

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

    df["ivwap_short"] = result
    return df


# ---------- FIRST SIGNAL ----------
def first_signal(group):
    group = group.sort_values("trade_exec_time")
    matched = group[group["signal"]]
    return matched.iloc[0] if not matched.empty else None


import time
from datetime import timedelta


def run_strategy_until_deadline(
    total_hours=6,
    short_window=5,
    spot_threshold=0,
    check_interval_seconds=60,
):
    now_utc = pd.Timestamp.now(tz="UTC")


    delivery_hour = now_utc.ceil("1h") + timedelta(hours=3)
    deadline = delivery_hour - timedelta(hours=3)

    print("Delivery:", delivery_hour)
    print("Deadline:", deadline)

    while pd.Timestamp.now(tz="UTC") < deadline:

        print("Checking for signal...")

        df_trade = fetch_data(sql_module.sql_trade_now, total_hours, "trade")
        df_spot = fetch_data(sql_module.sql_spot_prices, total_hours, "spot")
        df_consumption = fetch_data(sql_module.sql_consumption, total_hours, "consumption")
        df_solar = fetch_data(sql_module.sql_solar, total_hours, "solar")

        df_trade = add_grouped_ivwap(df_trade, short_window)

        df = df_trade.merge(df_spot, on="datetime_begin", how="left")
        df["spot"] = pd.to_numeric(df["spot"], errors="coerce")

        df["signal"] = df["ivwap_short"] >= (df["spot"] * (1 + spot_threshold))

        df_signals = (
            df.loc[df["signal"]]
            .sort_values(["datetime_begin", "trade_exec_time"])
            .drop_duplicates(subset=["datetime_begin"], keep="first")
            .reset_index(drop=True)
        )

        if not df_signals.empty:
            df_signals = df_signals.merge(df_consumption, on="datetime_begin", how="left")
            df_signals = df_signals.merge(df_solar, on="datetime_begin", how="left")

            df_signals = df_signals[
                df_signals["datetime_begin"] == delivery_hour
            ]

            mask_profit = (
                (df_signals["forecasted_delta"] < -300)
                | (df_signals["delta_solar_id_da"] < -300)
            )

            result = df_signals.loc[mask_profit].copy()

            if not result.empty:
                print("Signal found ✅")

                result = result[["datetime_begin"]]
                result["dim_data_provider_sk"] = "Inductive Energy"
                result["dim_power_price_area_sk"] = "HU"
                result["dim_granularity_sk"] = "15_MIN"
                result["dim_value_type_sk"] = "price"
                result["dim_strategy_sk"] = "HU_MOMENTUM_XBID_IMB"
                result["dim_product_type_sk"] = "XBID"
                result["dim_signal_type_sk"] = "DIRECTION"
                result["dim_direction_sk"] = "BUY"
                result["fact_value"] = 1
                result["dim_value_unit_sk"] = "INDICATOR"

                mask = [
                    'dim_data_provider_sk','dim_power_price_area_sk','dim_granularity_sk',
                    'dim_value_type_sk','dim_strategy_sk','dim_product_type_sk',
                    'dim_signal_type_sk','dim_direction_sk','fact_value',
                    'dim_value_unit_sk','datetime_begin'
                ]

                return result[mask], True

        # wait before checking again
        time.sleep(check_interval_seconds)

    print("Deadline reached ❌ No signal")

    return pd.DataFrame(), False



if __name__ == "__main__":
    data = run_strategy_until_deadline()

