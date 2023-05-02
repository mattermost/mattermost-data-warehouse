select 
    id as identifies_id,
    user_id as portal_user_id,
    context_traits_portal_customer_id,
    portal_customer_id
from 
{{ ref('base_portal_prod__identifies') }}

