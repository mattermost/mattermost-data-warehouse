with source as (

    select * from {{ source('salesforce', 'userrole') }}

),

renamed as (

    select
        id as user_role_id,

        -- Foreign keys
        parentroleid as parent_role_id,

        -- Properties
        name,
        caseaccessforaccountowner as case_access_for_account_owner,
        contactaccessforaccountowner as contact_access_for_account_owner,
        developername as developer_name,
        mayforecastmanagershare as may_forecast_manager_share,
        opportunityaccessforaccountowner as opportunity_access_for_account_owner,
        portaltype as portal_type,
        rollupdescription as rollup_description,

        -- Metadata
        lastmodifiedbyid as last_modified_by_id,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at

        -- Stitch columns omitted

    from source

)

select * from renamed
