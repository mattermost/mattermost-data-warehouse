select
    distinct
        license_id,
        issued_at,
        expire_at
from
    {{ ref('stg_licenses__licenses') }}
where
    license_id is not null
    and license_name in ('E10', 'E20', 'enterprise', 'professional')
