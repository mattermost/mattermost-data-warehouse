
with source as (

    select * from {{ source('salesforce', 'campaign') }}

),

renamed as (

    select
        id as campaign_id,

        -- Foreign keys
        createdbyid as created_by_id,
        lastmodifiedbyid as last_modified_by_id,
        ownerid as owner_id,
        parentid as parent_id,

        -- Details
        name,
        description,
        status,
        isactive as is_active,
        startdate as start_at,
        enddate as end_at,

        -- Planning
        numbersent as number_sent,
        expectedresponse as expected_response,
        type,
        budgetedcost as budgeted_cost,
        actualcost as actual_cost,
        expectedrevenue as expected_revenue,

        -- Statistics
        amountallopportunities as amount_all_opportunities,
        amountwonopportunities as amount_won_opportunities,
        hierarchyactualcost as hierarchy_actual_cost,
        hierarchyamountallopportunities as hierarchy_amount_all_opportunities,
        hierarchyamountwonopportunities as hierarchy_amount_won_opportunities,
        hierarchybudgetedcost as hierarchy_budgeted_cost,
        hierarchyexpectedrevenue as hierarchy_expected_revenue,
        hierarchynumberofcontacts as hierarchy_number_of_contacts,
        hierarchynumberofconvertedleads as hierarchy_number_of_converted_leads,
        hierarchynumberofleads as hierarchy_number_of_leads,
        hierarchynumberofopportunities as hierarchy_number_of_opportunities,
        hierarchynumberofresponses as hierarchy_number_of_responses,
        hierarchynumberofwonopportunities as hierarchy_number_of_won_opportunities,
        hierarchynumbersent as hierarchy_number_sent,
        numberofcontacts as number_of_contacts,
        numberofconvertedleads as number_of_converted_leads,
        numberofleads as number_of_leads,
        numberofopportunities as number_of_opportunities,
        numberofresponses as number_of_responses,
        numberofwonopportunities as number_of_won_opportunities,

        -- Metadata
        createddate as created_at,
        isdeleted as is_deleted,
        lastmodifieddate as last_modified_at,
        lastvieweddate as last_viewed_at,
        lastreferenceddate as last_referenced_at,
        systemmodstamp as system_modstamp_at,

        -- Custom columns
        campaign_offer_detail__c,
        campaign_offer__c,
        dscorgpkg__suppress_from_discoverorg__c,
        triggers_mql__c

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
where
    not is_deleted
