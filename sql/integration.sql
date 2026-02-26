WITH w_raw AS (
  SELECT
    measured_at,
    CAST(measured_at AT TIME ZONE 'Pacific/Auckland' AS DATE) AS local_date,
    weight_kg
  FROM read_parquet('data/raw_withings_weight.parquet')
),
cte_weight AS (
  SELECT
    local_date AS date,
    weight_kg
  FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY local_date
        ORDER BY measured_at DESC
      ) AS rn
    FROM w_raw
  )
  WHERE rn = 1
),
cte_steps AS (
  SELECT
    CAST(date AS DATE) AS date,
    steps
  FROM read_parquet('data/raw_fitbit_steps_daily.parquet')
),
cte_sleep AS (
  SELECT
    CAST(date AS DATE) AS date,
    minutes_asleep,
    minutes_deep,
    minutes_rem,
    minutes_light,
    efficiency
  FROM read_parquet('data/raw_fitbit_sleep_daily.parquet')
),
cte_heart AS (
  SELECT
    CAST(date AS DATE) AS date,
    resting_hr
  FROM read_parquet('data/raw_fitbit_heart_daily.parquet')
)
SELECT
  w.date,
  w.weight_kg,
  stp.steps,
  hrt.resting_hr,
  slp.minutes_asleep,
  slp.minutes_deep,
  slp.minutes_rem,
  slp.minutes_light,
  slp.efficiency
FROM cte_weight w
LEFT JOIN cte_steps stp ON stp.date = w.date
LEFT JOIN cte_sleep slp ON slp.date = w.date
LEFT JOIN cte_heart hrt ON hrt.date = w.date
ORDER BY w.date DESC;