{{config({
    "materialized": 'incremental',
    "schema": "util",
    "tags":["nightly"],
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

     telemetry  AS (
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
     ),

    telemetry_columns AS (
        SELECT
          table_catalog
        , table_schema
        , table_name
        , column_name
        , rank
        , data_type
        , ordinal_position
        , min(snapshot_date)                                                                       AS date_added
        FROM telemetry
        WHERE is_new
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    )

    SELECT *
    FROM telemetry_columns
    {% if is_incremental() %}

    WHERE date_added > (SELECT MAX(date_added) FROM {{this}})

    {% endif %}
