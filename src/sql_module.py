sql_trade = '''
   select
    dlvrystart as datetime_begin,
        trade_exec_time as trade_exec_time,
                        prod,
        ptc.px::float / 100.0 as price,
                                    (ptc.qty::float / 1000.0) AS qt
    from pblc_trade_conf_hupx ptc
    join dimcontractinfo di
        on ptc.contract_id = di.contract_id
    where prod = 'XBID_Hour_Power'
      and di.dlvrystart >= now() - interval '{total_hours} hours'
      and trade_exec_time <= di.dlvrystart -  interval '2 hour'
   and trade_exec_time >= di.dlvrystart -  interval '4 hour'

'''




sql_spot_prices = '''

SELECT
    date_trunc('hour', datetime_begin) AS datetime_begin,
    AVG(fact_value) AS spot
FROM (
    SELECT
        datetime_begin,
        fact_value,
        row_insert_datetime,
        RANK() OVER (
            PARTITION BY datetime_begin
            ORDER BY row_insert_datetime DESC
        ) AS row_rank
    FROM dpv.vfact_day_ahead
    WHERE datetime_begin >= (now() AT TIME ZONE 'Europe/Copenhagen') - interval '{total_hours}  hours'
      AND auction = 'DA'
      AND granularity = '15_MIN'
      AND power_price_area = 'HU'
      AND trading_venue = 'EPEX'
      AND value_unit = 'EUR/MWh'
      AND value_type = 'price'
      AND data_provider = 'ENTSOE'
      AND own_trade = false
) AS t
WHERE row_rank = 1
GROUP BY date_trunc('hour', datetime_begin)
ORDER BY datetime_begin

'''


sql_consumption = '''

WITH forecast_ranked AS (
    SELECT
        datetime_begin  as datetime_begin,
        fact_value,
        ROW_NUMBER() OVER (
            PARTITION BY datetime_begin
            ORDER BY row_insert_datetime DESC
        ) AS rn
    FROM dpv.vfact_consumption_forecast
    WHERE power_grid = 'MAVIR'
      AND data_provider = 'MAVIR'
    AND datetime_begin >= (now() AT TIME ZONE 'Europe/Copenhagen') - interval '{total_hours}  hours'
      AND value_type = 'LOAD_NET'
),
actual_ranked AS (
    SELECT
        datetime_begin  as datetime_begin,
        fact_value,
        ROW_NUMBER() OVER (
            PARTITION BY datetime_begin
            ORDER BY row_insert_datetime DESC
        ) AS rn
    FROM dpv.vfact_consumption_actual
    WHERE power_grid = 'MAVIR'
      AND data_provider = 'MAVIR'
      AND datetime_begin >= (now() AT TIME ZONE 'Europe/Copenhagen') - interval '{total_hours}  hours'
      AND value_type = 'LOAD_NET'
),
base_qh AS (
    SELECT
        datetime_begin,
        MAX(fact_value) FILTER (WHERE indicator = 'actual') AS actual,
        MAX(fact_value) FILTER (WHERE indicator = 'forecast') AS forecast
    FROM (
        SELECT datetime_begin, fact_value, 'forecast' AS indicator
        FROM forecast_ranked
        WHERE rn = 1

        UNION ALL

        SELECT datetime_begin, fact_value, 'actual' AS indicator
        FROM actual_ranked
        WHERE rn = 1
    ) a
    GROUP BY datetime_begin
),
base_hourly AS (
    SELECT
        date_trunc('hour', datetime_begin) AS datetime_begin,
        AVG(actual) AS actual,
        AVG(forecast) AS forecast
    FROM base_qh
    GROUP BY 1
),
delta_calc AS (
    SELECT
        datetime_begin,
        actual,
        forecast,
        CASE
            WHEN actual IS NOT NULL AND actual <> 0
                THEN (forecast - actual) / actual::numeric * 100
            ELSE NULL
        END AS pct_delta
    FROM base_hourly
),
grp AS (
    SELECT
        *,
        COUNT(pct_delta) OVER (
            ORDER BY datetime_begin
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS delta_grp
    FROM delta_calc
),
filled AS (
    SELECT
        datetime_begin,
        actual,
        forecast,
        pct_delta,
        MAX(pct_delta) OVER (PARTITION BY delta_grp) AS pct_delta_filled
    FROM grp
)
SELECT
    datetime_begin,
    actual,
    forecast,
    pct_delta,
    pct_delta_filled,
    CASE
        WHEN pct_delta_filled IS NOT NULL
             AND (1 + pct_delta_filled / 100.0) <> 0
        THEN forecast / (1 + pct_delta_filled / 100.0)
        ELSE NULL
    END AS estimated_actual,
    CASE
        WHEN pct_delta_filled IS NOT NULL
             AND (1 + pct_delta_filled / 100.0) <> 0
        THEN forecast - (forecast / (1 + pct_delta_filled / 100.0))
        ELSE NULL
    END AS forecasted_delta
FROM filled
ORDER BY datetime_begin

'''