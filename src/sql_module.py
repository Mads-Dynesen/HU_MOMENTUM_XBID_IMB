sql_trade = '''
   select
        dlvrystart as datetime_begin,
        trade_exec_time,
                        prod,
        ptc.px::float / 100.0 as price,
                                    (ptc.qty::float / 1000.0) AS qt
    from pblc_trade_conf_hupx ptc
    join dimcontractinfo di
        on ptc.contract_id = di.contract_id
    where prod = 'XBID_Hour_Power'
      and di.dlvrystart >= (now() at time zone 'Europe/Copenhagen') - interval '{total_hours} hours'
      and di.dlvrystart < (now() at time zone 'Europe/Copenhagen') + interval '2 hours'
      and ptc.trade_exec_time <= di.dlvrystart - interval '2 hours'



'''

sql_trade_now = '''
   select
        dlvrystart as datetime_begin,
        trade_exec_time,
                        prod,
        ptc.px::float / 100.0 as price,
                                    (ptc.qty::float / 1000.0) AS qt
    from pblc_trade_conf_hupx ptc
    join dimcontractinfo di
        on ptc.contract_id = di.contract_id
    where prod = 'XBID_Hour_Power'
      and di.dlvrystart >= (now() at time zone 'Europe/Copenhagen') + interval '1 hours'
      and di.dlvrystart < (now() at time zone 'Europe/Copenhagen') + interval '2 hours'
      and ptc.trade_exec_time <= di.dlvrystart - interval '2 hours'

'''

sql_spot_prices = '''

select
    datetime_begin,
    fact_value as spot

from (select datetime_begin,
             fact_value,
             'spot' as indicator,
             row_insert_datetime,
             RANK() OVER (PARTITION BY datetime_begin ORDER BY row_insert_datetime DESC) as row_rank
      from dpv.vfact_day_ahead
      where datetime_begin >= (now() at time zone 'Europe/Copenhagen') - interval '{total_hours} hours'
            and datetime_begin <= (now() at time zone 'Europe/Copenhagen') + interval '{total_hours} hours'
        and auction = 'DA'
        and granularity = '15_MIN'
        and power_price_area = 'HU'
        and trading_venue = 'EPEX'
        and value_unit = 'EUR/MWh'
        and value_type = 'price'
        and data_provider = 'ENTSOE') as t where row_rank = 1

'''


sql_imbalance_price = '''

select datetime_begin, avg(fact_value) as imbalance_price
from (
    select
        date_trunc('hour', datetime_begin) as datetime_begin,
        fact_value / 387.0 as fact_value
    from (
        select *,
               rank() over (
                   partition by datetime_begin, value_type
                   order by row_insert_datetime asc
               ) as row_rank
        from dpv.vfact_imbalance
        where datetime_begin >= (now() at time zone 'Europe/Copenhagen') - interval '{total_hours} hours'
            and datetime_begin <= (now() at time zone 'Europe/Copenhagen') + interval '{total_hours} hours'
          and power_price_area = 'HU'
          and granularity = '15_MIN'
          and data_provider = 'ENTSOE'
          and value_type = 'imbalance_price_long'
    ) a
    where row_rank = 1
) b group by datetime_begin

'''



sql_consumption = '''

WITH forecast_ranked AS (
    SELECT
        datetime_begin,
        fact_value,
        ROW_NUMBER() OVER (
            PARTITION BY datetime_begin
            ORDER BY row_insert_datetime DESC
        ) AS rn
    FROM dpv.vfact_consumption_forecast
    WHERE power_grid = 'MAVIR'
      AND data_provider = 'MAVIR'
      AND datetime_begin >= (now() AT TIME ZONE 'Europe/Copenhagen') - interval '{total_hours} hours'
      AND datetime_begin <= (now() AT TIME ZONE 'Europe/Copenhagen') + interval '{total_hours} hours'
      AND value_type = 'LOAD_NET'
),
actual_ranked AS (
    SELECT
        datetime_begin,
        fact_value,
        ROW_NUMBER() OVER (
            PARTITION BY datetime_begin
            ORDER BY row_insert_datetime DESC
        ) AS rn
    FROM dpv.vfact_consumption_actual
    WHERE power_grid = 'MAVIR'
      AND data_provider = 'MAVIR'
      AND datetime_begin >= (now() AT TIME ZONE 'Europe/Copenhagen') - interval '{total_hours} hours'
      AND datetime_begin <= (now() AT TIME ZONE 'Europe/Copenhagen') + interval '{total_hours} hours'
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

sql_solar = '''

WITH da_ranked AS (
    SELECT
        datetime_begin,
        fact_value,
        ROW_NUMBER() OVER (
            PARTITION BY datetime_begin
            ORDER BY row_insert_datetime DESC
        ) AS rn
    FROM dpv.vfact_production_forecast
    WHERE power_grid = 'MAVIR'
      AND data_provider = 'ENAPPSYS'
      AND datetime_begin >= (now() AT TIME ZONE 'Europe/Copenhagen') - interval '{total_hours} hours'
      AND datetime_begin <= (now() AT TIME ZONE 'Europe/Copenhagen') + interval '{total_hours} hours'
      AND production_type = 'SOLAR'
      AND forecast_provider = 'ENAPPSYS'
      AND forecast_type = 'DA'
),
id_ranked AS (
    SELECT
        datetime_begin,
        fact_value,
        ROW_NUMBER() OVER (
            PARTITION BY datetime_begin
            ORDER BY row_insert_datetime DESC
        ) AS rn
    FROM dpv.vfact_production_forecast
    WHERE power_grid = 'MAVIR'
      AND data_provider = 'ENAPPSYS'
      AND datetime_begin >= (now() AT TIME ZONE 'Europe/Copenhagen') - interval '{total_hours} hours'
      AND datetime_begin <= (now() AT TIME ZONE 'Europe/Copenhagen') + interval '{total_hours} hours'
      AND production_type = 'SOLAR'
      AND forecast_type = 'ID'
),
base AS (
    SELECT
        datetime_begin,
        MAX(fact_value) FILTER (WHERE indicator = 'da') AS da_forecast,
        MAX(fact_value) FILTER (WHERE indicator = 'id') AS id_forecast
    FROM (
        SELECT datetime_begin, fact_value, 'da' AS indicator
        FROM da_ranked
        WHERE rn = 1

        UNION ALL

        SELECT datetime_begin, fact_value, 'id' AS indicator
        FROM id_ranked
        WHERE rn = 1
    ) a
    GROUP BY datetime_begin
)
SELECT
    date_trunc('hour', datetime_begin) AS datetime_begin,
    AVG(id_forecast - da_forecast) AS delta_solar_id_da
FROM base
GROUP BY 1
ORDER BY 1

'''

