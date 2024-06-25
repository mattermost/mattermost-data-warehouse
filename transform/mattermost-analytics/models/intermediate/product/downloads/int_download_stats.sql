select
    *
from
    {{ ref('stg_releases__log_entries') }}