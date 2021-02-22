{{config({
    "materialized": 'incremental',
    "schema": "util"
  })
}}

-- New Columns Added to Telemetry Relations in raw database and when they were added
WITH snapshot AS (
    SELECT
        snapshot_date
      , table_catalog
      , table_schema
      , table_name
      , column_name
      , data_type
      , ordinal_position
    FROM {{ ref('raw_warehouse_columns_hist') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
                 ),

     telemetry_columns  AS (
         SELECT
             snapshot_date
           , table_catalog
           , table_schema
           , table_name
           , column_name
           , data_type
           , ordinal_position
           , min(snapshot_date) OVER (PARTITION BY table_schema, table_name, column_name) >
             min(snapshot_date) OVER ()
                                                                                                           AS is_new
           , row_number() OVER (PARTITION BY table_schema, table_name, column_name ORDER BY snapshot_date) AS rank
           , lag(column_name)
                 OVER (PARTITION BY table_schema, table_name, column_name ORDER BY snapshot_date)          AS prev_day_value
         FROM snapshot
     )

SELECT
    table_catalog
  , table_schema
  , table_name
  , column_name
  , rank
  , data_type
  , ordinal_position
  , min(snapshot_date)                                                                       AS date_added
FROM telemetry_columns
WHERE is_new
GROUP BY 1, 2, 3, 4, 5, 6, 7
{% if is_incremental() %}

HAVING MIN(snapshot_date) > (SELECT MAX(snapshot_date) FROM {{this}})

{% endif %}