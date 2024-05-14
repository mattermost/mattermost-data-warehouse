with subscription_history as (
    select 
        subscription_history_event_id
        ,  subscription_id
        ,  licensed_seats
        , created_at

    from {{ ref('stg_cws__subscription_history') }}
)

select * from subscription_history