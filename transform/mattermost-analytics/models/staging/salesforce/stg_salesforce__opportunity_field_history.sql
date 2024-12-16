with source as (

    select * from {{ source('salesforce', 'opportunityfieldhistory') }}

),

renamed as (

    select
        id as oportunity_field_history_id,

        -- Foreign keys
        opportunityid as opportunity_id,

        -- Metadata
        createddate as created_date,
        isdeleted as is_deleted,

        -- Values
        field,
        newvalue__bo,
        newvalue__de,
        newvalue__fl,
        newvalue__st,
        oldvalue__bo,
        oldvalue__de,
        oldvalue__fl,
        oldvalue__st,

        -- Stitch columns omitted

    from source

)

select * from renamed
