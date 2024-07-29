select
    {{ dbt_utils.star(ref('int_known_licenses')) }}
    , expire_at > current_date as is_expired
from
    {{ ref('int_known_licenses') }}