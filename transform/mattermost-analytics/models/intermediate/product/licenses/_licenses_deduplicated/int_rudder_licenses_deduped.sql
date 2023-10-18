-- Deduplicate licenses reported by Rudderstack, identifying potential error data.
select
    license_id,
    min(issued_at) as min_issued_at,
    max(issued_at) as max_issued_at,
    case
        when min_issued_at = max_issued_at then max_issued_at else null
    end as issued_at,
    min(starts_at) as min_starts_at,
    max(starts_at) as max_starts_at,
    case
        when min_starts_at = max_starts_at then max_starts_at else null
    end as starts_at,
    min(expire_at) as min_expire_at,
    max(expire_at) as max_expire_at,
    case
        when min_expire_at = max_expire_at then max_expire_at else null
    end as expire_at,
    ARRAY_UNIQUE_AGG(license_name) as license_names,
    min(licensed_seats) as min_licensed_seats,
    max(licensed_seats) as max_licensed_seats,
    case
        when min_licensed_seats = max_licensed_seats then max_licensed_seats else null
    end as licensed_seats,
    count(distinct expire_at) as count_expiration_dates
from
    {{ ref('stg_mm_telemetry_prod__license') }}
where
    license_id is not null
    and license_name in ('E10', 'E20', 'enterprise', 'professional')
group by license_id