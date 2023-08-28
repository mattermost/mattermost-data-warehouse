
with source as (

    select * from {{ source('cws', 'license') }}

),

renamed as (

    select
        id as license_id

        -- License info
        , ispending as is_pending
        , case
            when subscriptionid like 'non-subscription license for%' then null
            else subscriptionid
        end as subscription_id
        , extract_license_data(payload) as _license


         -- Metadata
        , to_timestamp(createdat / 1000) as created_at

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed

