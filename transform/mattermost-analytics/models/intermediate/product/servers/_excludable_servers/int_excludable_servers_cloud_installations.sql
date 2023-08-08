-- Servers marked as cloud from telemetry but no matching stripe data
select
    distinct sib.server_id,
    case
        -- Telemetry indicates an installation id but it doesn't exist in stripe
        when sib.installation_id is not null and s.cws_installation is null then 'No Stripe Installation Found'
        -- Installation exists but email belongs to an internal user
        when
            lower(split_part(c.email, '@', 2)) in ('mattermost.com', 'adamcgross.com', 'hulen.com')
            or lower(c.email) IN ('ericsteven1992@gmail.com', 'eric.nelson720@gmail.com') then 'Internal Email'
        else null
    end as reason
from
    {{ ref('_int_server_installation_id_bridge')}} sib
    left join {{ ref('stg_stripe__subscriptions')}} s on sib.installation_id = s.cws_installation
    left join {{ ref('stg_stripe__customers' )}} c on s.customer_id = c.customer_id and s.cws_installation is not null
where
    reason is not null