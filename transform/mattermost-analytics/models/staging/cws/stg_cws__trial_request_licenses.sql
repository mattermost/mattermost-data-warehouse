with source as (

    select * from {{ source('cws', 'trialrequestlicenses') }}

),

renamed as (

    select
        licenseid as license_id,
        trialrequestid as trial_request_id

        -- Stitch columns omitted

    from source

)

select * from renamed
