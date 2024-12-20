with source as (

    select * from {{ source('portal_prod', 'pageview_create_workspace') }}

),

renamed as (

    select
    id as pageview_id,
    user_id,
    event as event_table,
    timestamp

    from source

)

select * from renamed