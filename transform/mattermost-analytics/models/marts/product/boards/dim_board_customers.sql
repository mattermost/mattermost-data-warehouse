select
    sib.server_id,
    sib.installation_id,
    c.name,
    c.contact_first_name,
    c.contact_last_name,
    c.email,
    c.customer_id as stripe_customer_id,
    s.cws_dns,
    s.cws_installation as installation_id,
    s.edition as license_name

from
    {{ ref('_int_server_installation_id_bridge')}} sib
    join {{ ref('stg_stripe__subscriptions')}} s on sib.installation_id = s.cws_installation
    join {{ ref('stg_stripe__customers' )}} c on s.customer_id = c.customer_id and s.cws_installation is not null
where
    s.cws_installation is not null
    and sib.server_id in (select distinct server_id from {{ ref('int_boards_active_days_spined') }})