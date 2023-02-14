{{
    config({
        "tags":"hourly",
    })
}}

WITH tracks AS (
    SELECT
        {{ dbt_utils.star(ref('base_mattermost_docs__tracks')) }}
    FROM
        {{ ref ('base_mattermost_docs__tracks') }}
)
SELECT
     id            AS event_id
     , event       AS event_table
     , event_text  AS event_name
     , user_id     AS user_id
     , received_at AS received_at

FROM tracks