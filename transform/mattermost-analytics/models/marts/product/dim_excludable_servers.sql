
SELECT
    {{ dbt_utils.star(ref('int_excludable_servers')) }}
FROM
    {{ ref ('int_excludable_servers') }}
