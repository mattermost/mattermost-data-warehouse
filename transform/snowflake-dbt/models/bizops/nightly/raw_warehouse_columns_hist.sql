{{config({
    "materialized": "incremental",
    "schema": "bizops",
    "tags":["nightly"],
  })
}}

WITH raw_warehouse_columns_hist AS (
    SELECT 
        CURRENT_DATE AS SNAPSHOT_DATE
      , TABLE_CATALOG
      , TABLE_SCHEMA
      , TABLE_NAME
      , COLUMN_NAME
      , ORDINAL_POSITION
      , COLUMN_DEFAULT
      , IS_NULLABLE
      , DATA_TYPE
      , CHARACTER_MAXIMUM_LENGTH
      , CHARACTER_OCTET_LENGTH
      , NUMERIC_PRECISION
      , NUMERIC_PRECISION_RADIX
      , NUMERIC_SCALE
      , DATETIME_PRECISION
      , INTERVAL_TYPE
      , INTERVAL_PRECISION
      , MAXIMUM_CARDINALITY
    FROM {{ source('information_schema', 'columns') }}
    {% if is_incremental() %}

    WHERE CURRENT_DATE > (SELECT MAX(SNAPSHOT_DATE) FROM {{this}} )
    
    {% endif %}
    {{ dbt_utils.group_by(n=18)}}
)

SELECT *
FROM raw_warehouse_columns_hist