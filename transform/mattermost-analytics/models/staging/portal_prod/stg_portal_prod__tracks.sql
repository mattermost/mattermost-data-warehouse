
WITH tracks AS (
    SELECT
        {{ dbt_utils.star(ref('base_portal_prod__tracks')) }}
    FROM
        {{ ref ('base_portal_prod__tracks') }}
)
SELECT
     id                      AS event_id
     , event                 AS event_table
     , event_text            AS event_name
     , category              AS category
     , user_id               AS user_id
     , received_at           AS received_at
FROM
    tracks