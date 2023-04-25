
with source as (

    select * from {{ source('cws', 'trial_requests') }}

),

renamed as (

    select
        id as trial_request_id,

        -- User info
        name,
        contactfirstname as first_name,
        contactlastname as last_name,
        email,
        case
            when contactemail = '' then null
            else contactemail
        end as contact_email,

        -- Company info
        companycountry as country_name,
        companyname as company_name,
        companysize as company_size_bucket,

        -- Installation info
        serverid as server_id,
        sitename as site_name,
        siteurl as site_url,
        users as num_users,

        -- Trial info
        startdate as start_at,
        enddate as end_at,
        receiveemailsaccepted as is_receive_emails_accepted,
        termsaccepted as is_terms_accepted

        -- Stitch columns omitted

    from source

)

select * from renamed
