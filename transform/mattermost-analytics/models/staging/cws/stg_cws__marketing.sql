
with source as (

    select * from {{ source('cws', 'marketing') }}

),

renamed as (

    select
        id as marketing_id,

        -- User info
        email as email,
        subscribedcontent as subscribed_content,

        -- Marketing metadata
        serverid as server_id,
        to_timestamp(createdat) as created_at,
        to_timestamp(updatedat) as updated_at

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
