
with source as (

    select * from {{ source('salesforce', 'opportunitylineitem') }}

),

renamed as (

    select
        id as opportunity_line_item_id,

        -- Foreign keys
        createdbyid as created_by_id,
        lastmodifiedbyid as last_modified_by_id,
        opportunityid as opportunity_id,
        pricebookentryid as pricebook_entry_id,
        product2id as product2_id,

        -- Details
        name,
        description,
        productcode as product_code,
        servicedate as service_at,
        sortorder as sort_order,

        -- Accounting
        quantity,
        unitprice as unit_price,
        discount,
        listprice as list_price,
        totalprice as total_price,
        subtotal,

        -- Metadata
        createddate as created_at,
        isdeleted as is_deleted,
        lastmodifieddate as last_modified_at,
        systemmodstamp as system_modstamp_at,

        -- Custom columns
        amount_manual_override__c,
        coterm_expansion_amount__c,
        discount_calc__c,
        discounted_list_price__c,
        dwh_external_id__c,
        end_date__c,
        expansion_amount__c,
        invoice_id__c,
        invoice_status__c,
        is_prorated_expansion__c,
        leftover_expansion_amount__c,
        lineitemid__c,
        monthly_billing_amount__c,
        multi_amount__c,
        netsuite_invoice_id__c,
        new_amount__c,
        pricing_method__c,
        product_end_datef__c,
        product_line_type__c,
        product_start_datef__c,
        product_type__c,
        prorated_listprice__c,
        recalculate_sales_price__c,
        renewal_amount__c,
        renewal_end_date__c,
        renewal_multi_amount__c,
        renewal_start_date__c,
        renewed_by_opportunity_line_id__c,
        reseller_fee__c,
        revenue_type__c,
        sales_price_needs_to_be_updated__c,
        start_date__c,
        subs_id__c,
        subs_prev_version_id__c
        subs_version_id__c,
        term_months__c,
        total_amount__c,
        total_amount_total_price__c,
        total_price_with_annualized_expansion__c

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
where
    not is_deleted
