select
    distinct
        license_id,
        issued_at,
        starts_at,
        expire_at,
        license_name,
        licensed_seats
from
    {{ ref('stg_mattermost2__license') }}
where
    license_id is not null
    and license_name in ('E10', 'E20', 'enterprise', 'professional')
