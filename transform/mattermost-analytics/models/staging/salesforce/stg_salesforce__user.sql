
with source as (

    select * from {{ source('salesforce', 'user') }}

),

renamed as (

    select
        id as user_id,

        -- Foreign keys
        managerid as manager_id,
        profileid as profile_id,
        userroleid as user_role_id,

        -- Personal info
        name,
        firstname as first_name,
        lastname as last_name,
        title,
        username,
        email,
        employeenumber as employee_number,
        federationidentifier as federation_identifier,

        -- Company context
        department,
        division,

        -- Account details
        isactive as is_active,
        usertype as user_type,

        -- Metadata
        createddate as created_at,
        systemmodstamp as system_modstamp_at,

        -- Custom columns
        dwh_external_id__c,
        mattermost_handle__c,
        owner_type__c,
        sales_ops__c,
        sales_segment__c,
        start_date__c,
        system_type__c,
        territory__c,
        validation_exempt__c

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
