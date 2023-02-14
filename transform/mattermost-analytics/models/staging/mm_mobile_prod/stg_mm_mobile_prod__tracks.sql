{{
    config({
        "tags":"hourly",
    })
}}

WITH tracks AS (
    SELECT
        {{ dbt_utils.star(ref('base_mm_mobile_prod__tracks')) }}
    FROM
        {{ ref ('base_mm_mobile_prod__tracks') }}
)
SELECT
     id               AS event_id
     , event          AS event_table
     , event_text     AS event_name
     , type           AS type
     , user_id        AS server_id
     , user_actual_id AS user_id
     , received_at    AS received_at
FROM tracks