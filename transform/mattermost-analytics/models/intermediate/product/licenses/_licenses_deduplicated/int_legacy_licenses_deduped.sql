-- Deduplicate licenses from legacy source (S3)
select
    distinct
        license_id,
        issued_at,
        expire_at
from
    {{ ref('stg_licenses__licenses') }}
where
    license_id is not null