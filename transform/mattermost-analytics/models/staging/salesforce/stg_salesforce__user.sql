
with source as (

    select * from {{ source('salesforce', 'user') }}

),

renamed as (

    select
        id as user_id,
        createddate as created_date,
        department,
        division,
        email,
        employeenumber as employee_number,
        federationidentifier as federation_identifier,
        firstname as first_name,
        isactive as is_active,
        lastname as last_name,
        managerid as manager_id,
        name,
        profileid as profile_id,
        systemmodstamp,
        title,
        username,
        userroleid as user_role_id,
        usertype as user_type,

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
