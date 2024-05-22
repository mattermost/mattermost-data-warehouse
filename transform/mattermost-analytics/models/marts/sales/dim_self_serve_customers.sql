select
    customer_id
    , contact_first_name
    , contact_last_name
    , name
    , email
from 
    {{ ref('stg_stripe__customers') }}