select
    {{ dbt_utils.star(ref('stg_mattermost__version_release_dates'))}}
from
    {{ ref('stg_mattermost__version_release_dates') }}