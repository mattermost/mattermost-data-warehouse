
with source as (

    select * from {{ source('cws', 'trial_requests') }}

),

renamed as (

    select
        id,

        -- User info
        name,
        contactfirstname,
        contactlastname,
        email,
        contactemail,

        -- Company info
        companycountry,
        companyname,
        companysize,

        -- Installation info
        serverid,
        sitename,
        siteurl,
        users,

        -- Trial info
        startdate,
        enddate,
        receiveemailsaccepted,
        termsaccepted

        -- Stitch columns omitted

    from source

)

select * from renamed
