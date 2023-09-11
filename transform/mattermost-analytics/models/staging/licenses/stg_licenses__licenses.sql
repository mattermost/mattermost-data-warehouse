
with source as (

    select * from {{ source('licenses', 'licenses') }}

),

renamed as (

    select
        customerid as customer_id,
        company as company_name,
        email as contact_email,
        stripeid as stripe_customer_id,
        licenseid as license_id,
        to_timestamp_ntz(issuedat / 1000) as issued_at,
        to_timestamp_ntz(expiresat / 1000) as expire_at,

        -- Unknown semantics
        number
    from source

)

select * from renamed
