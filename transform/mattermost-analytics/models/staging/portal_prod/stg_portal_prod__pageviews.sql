WITH pageviews AS (
    SELECT
        {{ dbt_utils.star(ref('base_portal_prod__pageviews')) }}
    FROM
        {{ ref ('base_portal_prod__pageviews') }}
)
SELECT
    *
FROM
    pageviews limit 15;