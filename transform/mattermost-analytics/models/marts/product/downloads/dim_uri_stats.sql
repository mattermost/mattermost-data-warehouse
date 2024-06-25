select
    {{ dbt_utils.star(from=ref('int_download_stats_per_uri')) }}
from
    {{ ref('int_download_stats_per_uri') }}