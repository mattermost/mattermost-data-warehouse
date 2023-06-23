
with source as (

    select * from {{ source('cws', 'trial_requests') }}

),

renamed as (

    select
        id as trial_request_id,

        -- User info
        case
            when name = '' then null
            else name
        end as name,
        case
            when contactfirstname = '' then null
            else contactfirstname
        end as first_name,
        case
            when contactlastname = '' then null
            else contactlastname
        end as last_name,
        -- Attempt to extract first and last name from name.
        trim(name) as _name,
        case
            -- Empty string
            when _name = '' then null
            -- A whitespace exists in the string, split it
            when charindex(' ', _name) > 0 then substring(name, 1, charindex(' ', name) - 1)
            -- No whitespace, return the full string
            else _name
        end as extracted_first_name,
        case
            -- Empty string
            when _name = '' then null
            -- A whitespace exists
            when charindex(' ', _name) > 0 then substring(name, charindex(' ', name) + 1, len(name) - charindex(' ', name))
            -- No whitespace, can't find it
            else null
        end as extracted_last_name,
        email,
        case
            when contactemail = '' then null
            else contactemail
        end as contact_email,

        -- Company info
        companycountry as country_name,
        case
            when companyname = '' then null
            else companyname
        end as company_name,
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

select
    *
from
    renamed
where
    trial_request_id not in (
        -- Invalid records
        'uu6zjmatt38wzmhphs57rsbbro'
    )
