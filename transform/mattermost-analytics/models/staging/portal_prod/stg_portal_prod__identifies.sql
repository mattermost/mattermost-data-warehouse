WITH identifies as(
    SELECT
        {{ dbt_utils.star(source('portal_prod', 'identifies')) }}
    FROM
        {{ source('portal_prod', 'identifies') }}
)

SELECT 
    user_id,
    portal_customer_id,
    context_traits_portal_customer_id,
    received_at
FROM
    identifies