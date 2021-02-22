{{config({
    "materialized": 'incremental',
    "schema": "util"
  })
}}

-- Stores new Columns Added to Telemetry Relations (Count of snapshot date vs. previous date) 
WITH snapshot          AS (
    SELECT
        snapshot_date
      , table_catalog
      , table_schema
      , table_name
      , count(DISTINCT column_name) AS column_count
    FROM {{ ref('raw_warehouse_columns_hist') }}
    GROUP BY 1, 2, 3, 4
                          ),

     telemetry_tables AS (
         SELECT
             snapshot_date
           , table_catalog
           , table_schema
           , table_name
           , CASE WHEN lag(column_count)
                 OVER (PARTITION BY table_name, table_schema ORDER BY snapshot_date) is null then 'New Table' 
                 WHEN COALESCE(lag(column_count)
                 OVER (PARTITION BY table_name, table_schema ORDER BY snapshot_date), 0) > 0 AND 
                       COALESCE(lag(column_count)
                 OVER (PARTITION BY table_name, table_schema ORDER BY snapshot_date), 0) <> column_count THEN 'New Column' 
                 ELSE NULL END AS update_type
           , column_count
           , lag(column_count)
                 OVER (PARTITION BY table_name, table_schema ORDER BY snapshot_date) AS prev_day_column_count
         FROM snapshot
     )

SELECT *
FROM telemetry_tables
WHERE update_type IS NOT NULL
{% if is_incremental() %}

AND snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

{% endif %}