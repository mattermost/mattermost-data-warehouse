{{ config(
    severity = 'warn'
) }}

-- This test checks that the join between int_nps_feedback and int_nps_score generates a single row per combination of server_id and user_id.
-- If this test fails, it indicates that there are duplicates for the same combination.

-- Query to check for duplicates
WITH joined_rows AS (
    SELECT
        nf.server_id AS server_id,
        nf.user_id AS user_id,
        COUNT(*) AS row_count
    FROM {{ ref('int_nps_feedback') }} nf
    LEFT JOIN {{ ref('int_nps_score') }} ns
    ON nf.server_id = ns.server_id AND nf.user_id = ns.user_id AND nf.event_date = ns.event_date
    GROUP BY nf.server_id, nf.user_id
)
SELECT
    server_id,
    user_id,
    row_count
FROM joined_rows
WHERE row_count > 1;
