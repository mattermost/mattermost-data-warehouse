SELECT
    {{ dbt_utils.star(source('portal_prod', 'IDENTIFIES')) }}
FROM
    {{ ref ('base_portal_prod__tracks') }}