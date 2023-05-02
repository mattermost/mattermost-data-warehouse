

SELECT
    {{ dbt_utils.star(ref('base_portal_prod__tracks')) }}
FROM
    {{ ref ('base_portal_prod__tracks') }}
