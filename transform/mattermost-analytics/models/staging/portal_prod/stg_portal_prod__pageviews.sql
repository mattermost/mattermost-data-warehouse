
with pageviews as(
    SELECT
        {{ dbt_utils.star(ref('base_portal_prod__tracks')) }}
    FROM
        {{ ref ('base_portal_prod__tracks') }}
)

select 
    id,
    user_id,
    event as event_table
from 
    pageviews

