with source as (

    select * from {{ source('portal_prod', 'identifies') }}

),

renamed as (

    select
        user_id,
        coalesce(portal_customer_id,context_traits_portal_customer_id) as portal_customer_id,
        timestamp
    
    from source

)

select * from renamed