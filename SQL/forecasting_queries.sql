CREATE OR REPLACE MODEL `{MODEL}`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='ts',
  time_series_data_col='price_mxn',
  time_series_id_col='asset',
  auto_arima=TRUE,
  data_frequency='DAILY'
) AS
WITH daily AS (
  SELECT
    asset,
    fecha,
    pulled_at_utc,
    price_mxn,
    ROW_NUMBER() OVER (PARTITION BY asset, fecha ORDER BY pulled_at_utc DESC) AS rn
  FROM `{SRC}`
  WHERE price_mxn IS NOT NULL
)
SELECT
  asset,
  TIMESTAMP(fecha, "America/Monterrey") AS ts,
  price_mxn
FROM daily
WHERE rn = 1
"""
client.query(train_sql).result()
print("✅ Modelo ARIMA (precio) entrenado:", MODEL)


def build_portfolio_value_forecast(horizon_days:int, dest_table_fqn:str):
    sql = f"""
    CREATE OR REPLACE TABLE `{dest_table_fqn}` AS
    WITH
    -- 1) Último snapshot del día por asset (fecha como DATE)
    last_per_day AS (
      SELECT asset, fecha_d, pulled_at_utc, total, price_mxn
      FROM (
        SELECT
          asset,
          SAFE_CAST(fecha AS DATE) AS fecha_d,
          pulled_at_utc,
          total,
          price_mxn,
          ROW_NUMBER() OVER (PARTITION BY asset, SAFE_CAST(fecha AS DATE) ORDER BY pulled_at_utc DESC) AS rn
        FROM `{SRC}`
      )
      WHERE rn = 1 AND fecha_d IS NOT NULL
    ),

    -- 2) Fecha más reciente del historial (por asset)
    last_day AS (
      SELECT asset, MAX(fecha_d) AS last_fecha
      FROM last_per_day
      GROUP BY asset
    ),

    -- 3) Último total (tokens actuales) por asset
    last_total AS (
      SELECT d.asset, d.total AS last_total, d.fecha_d AS last_fecha
      FROM last_per_day d
      JOIN last_day l
      ON d.asset = l.asset AND d.fecha_d = l.last_fecha
    ),

    -- 4) Staking semanal promedio (incrementos lunes vs lunes)
    --    DAYOFWEEK: 1=Domingo, 2=Lunes, ..., 7=Sábado
    mondays AS (
      SELECT
        asset,
        fecha_d,
        total,
        LAG(total) OVER (PARTITION BY asset ORDER BY fecha_d) AS prev_total
      FROM last_per_day
      WHERE EXTRACT(DAYOFWEEK FROM fecha_d) = 2
    ),
    monday_deltas AS (
      SELECT
        asset,
        GREATEST(total - prev_total, 0) AS monday_delta
      FROM mondays
      WHERE prev_total IS NOT NULL
    ),
    staking_weekly AS (
      SELECT
        asset,
        AVG(monday_delta) AS avg_weekly_delta_tokens
      FROM monday_deltas
      GROUP BY asset
    ),

    -- 5) Forecast diario de precios (ARIMA) a horizon_days
    price_fc AS (
      SELECT
        asset,
        DATE(forecast_timestamp, "America/Monterrey") AS fc_date,
        forecast_value AS price_mxn_forecast,
        prediction_interval_lower_bound AS price_mxn_lo,
        prediction_interval_upper_bound AS price_mxn_hi
      FROM ML.FORECAST(
        MODEL `{MODEL}`,
        STRUCT({horizon_days} AS horizon)
      )
    ),

    -- 6) Calendario diario por asset (desde mañana hasta horizonte)
    calendar AS (
      SELECT
        lt.asset,
        d AS fc_date
      FROM last_total lt,
      UNNEST(
        GENERATE_DATE_ARRAY(
          DATE_ADD(lt.last_fecha, INTERVAL 1 DAY),
          DATE_ADD(lt.last_fecha, INTERVAL {horizon_days} DAY)
        )
      ) AS d
    ),

    -- 7) Contar lunes transcurridos entre (last_fecha, fc_date]
    monday_count AS (
      SELECT
        c.asset,
        c.fc_date,
        (
          SELECT COUNTIF(EXTRACT(DAYOFWEEK FROM x) = 2)
          FROM UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(lt.last_fecha, INTERVAL 1 DAY), c.fc_date)) AS x
        ) AS mondays_since_last
      FROM calendar c
      JOIN last_total lt USING(asset)
    ),

    -- 8) Tokens futuros = tokens actuales + (lunes * avg_staking)
    tokens_fc AS (
      SELECT
        mc.asset,
        mc.fc_date,
        lt.last_total,
        IFNULL(sw.avg_weekly_delta_tokens, 0) AS avg_weekly_delta_tokens,
        mc.mondays_since_last,
        lt.last_total + mc.mondays_since_last * IFNULL(sw.avg_weekly_delta_tokens, 0) AS tokens_forecast
      FROM monday_count mc
      JOIN last_total lt USING(asset)
      LEFT JOIN staking_weekly sw USING(asset)
    ),

    -- 9) Valor futuro por asset + bandas
    by_asset AS (
      SELECT
        t.asset,
        t.fc_date,
        t.tokens_forecast,
        p.price_mxn_forecast,
        p.price_mxn_lo,
        p.price_mxn_hi,
        (t.tokens_forecast * p.price_mxn_forecast) AS value_mxn_forecast,
        (t.tokens_forecast * p.price_mxn_lo) AS value_mxn_lo,
        (t.tokens_forecast * p.price_mxn_hi) AS value_mxn_hi
      FROM tokens_fc t
      JOIN price_fc p
      ON p.asset = t.asset AND p.fc_date = t.fc_date
    ),

    total_portfolio AS (
      SELECT
        fc_date,
        SUM(value_mxn_forecast) AS portfolio_value_mxn_forecast,
        SUM(value_mxn_lo) AS portfolio_value_mxn_lo,
        SUM(value_mxn_hi) AS portfolio_value_mxn_hi
      FROM by_asset
      GROUP BY fc_date
    )

    SELECT
      b.*,
      tp.portfolio_value_mxn_forecast,
      tp.portfolio_value_mxn_lo,
      tp.portfolio_value_mxn_hi
    FROM by_asset b
    JOIN total_portfolio tp USING(fc_date)
    ORDER BY fc_date, asset
