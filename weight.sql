
SELECT 
    strftime(
        measured_at AT TIME ZONE 'Pacific/Auckland',
        '%Y-%m-%d %H:%M:%S'
    ) AS measured_nz_datetime,
    weight_kg,
    strftime(
        ingested_at AT TIME ZONE 'Pacific/Auckland',
        '%Y-%m-%d %H:%M:%S'
    ) AS ingested_nz_datetime
FROM raw_withings_weight
ORDER BY measured_at DESC;