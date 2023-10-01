select
    sib.server_id,
    c.name,
    c.contact_first_name,
    c.contact_last_name,
    c.email,
    c.customer_id as stripe_customer_id
from
    {{ ref('_int_server_installation_id_bridge')}} sib
    join {{ ref('stg_stripe__subscriptions')}} s on sib.installation_id = s.cws_installation
    join {{ ref('stg_stripe__customers' )}} c on s.customer_id = c.customer_id and s.cws_installation is not null
where
    s.cws_installation is not null
    and sib.server_id in (select distinct server_id from {{ ref('int_boards_active_days_spined') }})
qualify row_number() over (partition by server_id order by s.created_at desc) = 1