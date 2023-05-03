WITH identifies as (
    SELECT user_id, 
        coalesce(portal_customer_id, context_traits_portal_customer_id) as portal_customer_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY RECEIVED_AT) AS row_number
    from
        {{ ref('base_portal_prod__identifies') }}
    WHERE 
        coalesce(portal_customer_id, context_traits_portal_customer_id) IS NOT NULL
)

select 
    user_id,
    portal_customer_id
from 
    identifies
where 
    row_number = 1

