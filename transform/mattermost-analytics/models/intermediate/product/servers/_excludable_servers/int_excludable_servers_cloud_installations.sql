-- Cloud servers registered using mattermost related users.
select
    distinct sib.server_id,
    case
        when s.cws_installation is null then 'No Stripe Installation Found'
    end as reason
    -- There's a bug in version format in past model
from
    {{ ref('_int_server_installation_id_bridge')}} sib
    left join {{ ref('stg_stripe__subscriptions')}} s on sib.installation_id = s.cws_installation
    left join {{ ref('stg_stripe__customers' )}} c on s.customer_id = c.customer_id and s.cws_installation is not null
where
    reason is not null