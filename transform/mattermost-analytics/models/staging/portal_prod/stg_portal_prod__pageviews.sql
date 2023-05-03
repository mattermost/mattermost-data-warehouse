
with pageviews as(
    SELECT
        {{ dbt_utils.star(ref('base_portal_prod__tracks')) }}
    FROM
        {{ ref ('base_portal_prod__tracks') }}
)

select 
    id as pageview_id,
    user_id,
    event as event_table,
    received_at
from 
    pageviews

