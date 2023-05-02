SELECT
    {{ dbt_utils.star(source('portal_prod', 'identifies')) }}
FROM
    {{ source('portal_prod', 'identifies') }}