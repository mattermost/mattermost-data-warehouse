select
    server_id,
    when
        s.cws_installation is null then 'No Stripe Installation Found'
        else null
    end as missing_stripe_installation,
   case
        when
            lower(SPLIT_PART(c.email, '@', 2)) in ('mattermost.com', 'adamcgross.com', 'hulen.com')
            or lower(c.email) IN ('ericsteven1992@gmail.com', 'eric.nelson720@gmail.com') then 'Internal Email'
        else null
    end as internal_email,
    case when server_ip = '194.30.0.184' then 'Restricted IP' end as restricted_ip,
    case when is_enterprise_ready and has_run_unit_tests then 'Dev Build/Ran Tests' as dev_build
from {{ ref('stg_diagnostics__log_entries') }}