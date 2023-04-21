
with source as (

    select * from {{ source('salesforce', 'campaign') }}

),

renamed as (

    select
        id as campaign_id,
        actualcost as actual_cost,
        amountallopportunities as amount_all_opportunities,
        amountwonopportunities as amount_won_opportunities,
        budgetedcost as budgeted_cost,
        createdbyid as created_by_id,
        createddate as created_date,
        description,
        enddate as end_date,
        expectedresponse as expected_response,
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
        isactive as is_active,
        isdeleted as is_deleted,
        lastmodifiedbyid as last_modified_by_id,
        lastmodifieddate as last_modified_date,
        name,
        numberofcontacts as number_of_contacts,
        numberofconvertedleads as number_of_converted_leads,
        numberofleads as number_of_leads,
        numberofopportunities as number_of_opportunities,
        numberofresponses as number_of_responses,
        numberofwonopportunities as number_of_won_opportunities,
        numbersent as number_sent,
        ownerid as owner_id,
        parentid as parent_id,
        startdate as start_date,
        status,
        systemmodstamp,
        type,
        lastvieweddate as last_viewed_date,
        lastreferenceddate as last_referenced_date,
        expectedrevenue as expected_revenue,

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
