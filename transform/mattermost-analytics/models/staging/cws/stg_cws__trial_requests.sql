
with source as (

    select * from {{ source('cws_test', 'trial_requests') }}

),

renamed as (

    select
        id AS trial_request_id,
        companycountry AS country_name,
        companyname AS company_name,
        companysize AS company_size_bucket,
        contactemail AS contact_email,
        contactfirstname AS first_name,
        contactlastname AS last_name,
        email AS email,
        name AS username,
        receiveemailsaccepted is_receive_email_accepted,
        serverid AS server_id,
        sitename AS site_name,
        siteurl AS site_url,
        startdate AS start_date,
        enddate AS end_date,
        termsaccepted AS is_terms_accepted,
        users AS num_users
        -- Stitch columns omitted
    from source

)

select * from renamed
