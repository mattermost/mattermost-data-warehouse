with source as (
    select * from {{ source('cws', 'subscriptionhistory') }}

),

renamed as (
    select 
        id as subscription_history_event_id
        , subscriptionid as subscription_id
        , seats as licensed_seats
        , to_timestamp(createat) as created_at

    from source
)

select 
    *
from
    renamed