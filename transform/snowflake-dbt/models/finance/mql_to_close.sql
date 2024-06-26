{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":["nightly"]
  })
}}

with marketo_form_fills as (
    select 
        row_number() OVER (partition by split_part(SUBSTRING(form_fields, regexp_instr(form_fields, '"Email";s:[0-9]+:"'), length(form_fields)), '"',4) ORDER BY activitydate desc) as rank,
        activitydate::date as form_fill_date,
        split_part(SUBSTRING(form_fields, regexp_instr(form_fields, '"Email";s:[0-9]+:"'), length(form_fields)), '"',4) as email,
        primary_attribute_value
    from {{ source('staging','activities_fill_out_form') }}
    where primary_attribute_value IN (
                                      'Cloud Beta Trial',
                                      'Cloud Enterprise Quote Request',
                                      'Cloud Signup',
                                      'Contact Us',
                                      'Demo Request',
                                      'Enterprise Inquiry',
                                      'Government Inquiry',
                                      'Hipchat Contact',
                                      'Partner Program',
                                      'Quote Request',
                                      'Remote Work Offer',
                                      'Trial',
                                      '[NEW] Nonprofit Inquiry',
                                      '[NEW] Reseller Deal Reg'
        )
), lead_hist as (
    select
        row_number() OVER (partition by lead_sfid ORDER BY DATE desc) as rank_lead_hist,
        date,
        lead_sfid,
        additional_details
    from {{ ref('lead_status_hist') }}
    where lead_status_hist.status = 'MQL'
), lead_data_raw as (
    select
        row_number() OVER (partition by coalesce(additional_details, primary_attribute_value) ORDER BY coalesce(MOST_RECENT_MQL_DATE__C, FIRST_MQL_DATE__C) desc) as rank,
        lead.sfid,
        lead.email,
        lead.status,
        additional_details,
        coalesce(additional_details, primary_attribute_value, coalesce(MOST_RECENT_ACTION__C,'')|| ' - ' || coalesce(MOST_RECENT_ACTION_DETAIL__C,''), coalesce(FIRST_ACTION__C,'') || ' - ' || coalesce(FIRST_ACTION_DETAIL__C,'')) as mql_reason_combo,
        LEAD_SOURCE_TEXT__C,
        LEAD_SOURCE_DETAIL__C,
        FIRST_MQL_DATE__C,
        coalesce(MOST_RECENT_MQL_DATE__C, FIRST_MQL_DATE__C)                                as MOST_RECENT_MQL_DATE__C,
        MOST_RECENT_ACTION_DETAIL__C,
        PRIMARY_ATTRIBUTE_VALUE                                                             as form,
        form_fill_date,
        additional_details                                                                  as mql_reason,
        DATE                                                                                as mql_date,
        CONVERTEDCONTACTID
    from {{ ref('lead') }}
        left join marketo_form_fills on lower(lead.email) = lower(marketo_form_fills.email) and rank = 1
        left join lead_hist on lead.sfid = lead_hist.LEAD_SFID
    where coalesce(FIRST_MQL_DATE__C,MOST_RECENT_MQL_DATE__C) IS NOT NULL
    order by MOST_RECENT_MQL_DATE__C desc
), mql_details as (
    select CASE
               WHEN (mql_reason_combo IS NULL AND LEAD_SOURCE_TEXT__C = 'List Purchase') or mql_reason_combo like '%List Purchase%' or mql_reason_combo like '%Purchased List%' THEN 'List Purchase'
               WHEN (mql_reason_combo IS NULL AND LEAD_SOURCE_TEXT__C like '%Event%') OR mql_reason_combo like '%Event%' OR mql_reason_combo like '%Attend%' OR mql_reason_combo like '%Visit Booth%' THEN 'Event'
               WHEN mql_reason_combo LIKE 'E0 Download%' OR mql_reason_combo LIKE '%Download%E10%' THEN 'E0 Download'
               WHEN mql_reason_combo like '%Trial%' AND mql_reason_combo not like '%In-Product Trial Request%' THEN 'Trial Request - Website'
               WHEN mql_reason_combo = 'Contact Us' THEN 'Contact Us - General'
               WHEN mql_reason_combo like '%Download%' THEN 'Download'
               WHEN mql_reason_combo like '%Get Started%' THEN 'Contact Request - Demo'
               WHEN mql_reason_combo like 'Demo Request%' THEN 'Contact Request - Demo'
               WHEN mql_reason_combo like 'Webinar%' THEN 'Webinar'
               WHEN mql_reason_combo like 'Asset Download%' THEN 'Asset Download'
               WHEN mql_reason_combo like '%AA - %' or mql_reason_combo like '%Admin Advisor%' THEN 'Admin Advisor'
               WHEN mql_reason_combo = 'Cloud Enterprise Quote Request' THEN 'Cloud - Enterprise Quote Request'
               WHEN mql_reason_combo = 'Cloud - Ent/MM Workspace Creation' OR mql_reason_combo = 'Cloud PQL - Cloud Workspace Creation' THEN 'Cloud - Cloud Workspace Creation'
               WHEN mql_reason_combo = 'Cloud Signup' THEN 'Cloud - Beta'
               WHEN mql_reason_combo like '%Feature Request%' THEN 'Feature Request'
               WHEN mql_reason_combo like 'Training%' or mql_reason_combo like 'MM Academy' THEN 'Training'
               WHEN mql_reason_combo like '%In-Product Trial Request%' THEN 'Trial Request - In Product'
               WHEN mql_reason_combo like '%Partner%' OR mql_reason_combo like '%Reseller%' THEN 'Contact Request - Partner/Reseller'
               WHEN mql_reason_combo = 'Contact Request - In-Portal' THEN 'Contact Request - In-Portal Contact Us'
               WHEN mql_reason_combo like 'Outsourced Teleprospecting%' THEN 'Outsourced Teleprospecting'
               WHEN mql_reason_combo like '%Quote%' THEN 'Contact Request - Pricing'
               WHEN mql_reason_combo like '%Nonprofit%' THEN 'Contact Request - Nonprofit License'
               WHEN mql_reason_combo like '%Government%' THEN 'Contact Request - General'
               WHEN mql_reason_combo like '%Email Sign-Up%' THEN 'Email Sign-Up'
               WHEN mql_reason_combo in
                    ('Contact Request - ', 'Contact Us - General', 'Enterprise Inquiry', 'Enterprise Inquiry - General',
                     'Contact Us - General', 'Contact Us - Sales', 'Contact Request - Drift Chat',
                     'Contact Request - Hipchat Migration', 'Contact Request - Security Vulnerability',
                     'Contact Request - Solution Architect Meeting', 'Contact Support - Security Issue',
                     'Contact Support - Zendesk Sign-Ups', 'Contact Us - ') THEN 'Contact Request - General'
               ELSE mql_reason_combo END as mql_reason_final,
           *
    from lead_data_raw
), final_lead_details as (
    select CASE
               when mql_details.mql_reason_final in (
                                                     'Admin Advisor', 'Cloud - Beta',
                                                     'Cloud - Cloud Workspace Creation',
                                                     'Cloud - Enterprise Quote Request', 'Contact Request - Demo',
                                                     'Contact Request - General',
                                                     'Contact Request - In-Cloud Contact Us',
                                                     'Contact Request - In-Portal Contact Us',
                                                     'Contact Request - Nonprofit License',
                                                     'Contact Request - Partner/Reseller', 'Contact Request - Pricing',
                                                     'Contact Us - General', 'Download', 'E0 Download', 'Email Sign-Up',
                                                     'Event', 'Feature Request', 'Get Started Demo', 'List Purchase',
                                                     'Outsourced Teleprospecting', 'Remote Work Offer',
                                                     'Sales Generated - AE-Created', 'Training',
                                                     'Trial Request - In Product', 'Trial Request - Website', 'Webinar'
                   ) THEN mql_details.mql_reason_final
               ELSE 'Unknown' END as mql_reason
         , SFID
         , MOST_RECENT_MQL_DATE__C
         , status
         , CONVERTEDCONTACTID
    from mql_details

), a as (
    select
        final_lead_details.sfid as lead_sfid,
        final_lead_details.MOST_RECENT_MQL_DATE__C,
        account.sfid as account_sfid,
        opportunity.sfid as opportunity_sfid,
        opportunity.CREATEDDATE::date as createddate,
        mql_reason
    from final_lead_details
        join {{ ref('contact') }} on final_lead_details.CONVERTEDCONTACTID = contact.sfid
        join {{ ref('opportunitycontactrole') }} on contact.sfid = OPPORTUNITYCONTACTROLE.CONTACTID
        join {{ ref('opportunity_ext') }} on OPPORTUNITYCONTACTROLE.OPPORTUNITYID = OPPORTUNITY_SFID and MARKETING_GENERATED
        join {{ ref('opportunity') }} on OPPORTUNITY.sfid = OPPORTUNITY_EXT.OPPORTUNITY_SFID and final_lead_details.MOST_RECENT_MQL_DATE__C::date <= opportunity.CREATEDDATE::date
        join {{ ref('opportunitylineitem') }} on opportunity.SFID = OPPORTUNITYLINEITEM.opportunityid
        join {{ ref('account') }} on opportunity.accountid = ACCOUNT.sfid
    group by 1, 2, 3, 4, 5, 6
), order_mql_leads as (
    select
        row_number() OVER (partition by OPPORTUNITY_SFID ORDER BY MOST_RECENT_MQL_DATE__C desc) as rank,
        *
    from a
), lead_credit as (
    select lead_sfid, max(opportunity_sfid) as opportunity_sfid
    from order_mql_leads
    where rank = 1
    group by 1
), opportunity_info as (
    select final_lead_details.sfid                                                               as lead_sfid,
        lead.status,
        final_lead_details.MOST_RECENT_MQL_DATE__C::date                                      as mql_date,
        account.name as account_name,
        account.sfid as account_sfid,
        account.CUSTOMER_SEGMENTATION_TIER__C as tier,
        account.TERRITORY_SEGMENT__C as segment,
        account.COMPANY_TYPE__C as account_type,
        opportunity.name as opportunity_name,
        lead_credit.opportunity_sfid,
        opportunity.order_type__c,
        opportunity.type,
        opportunity.STATUS_WLO__C,
        opportunity.CLOSEDATE::date as closedate,
        opportunity.createddate::date as createddate,
        opportunity.AMOUNT,
        round(opportunity.AMOUNT / NULLIF(DATEDIFF('day', opportunity_ext.MIN_START_DATE, opportunity_ext.MAX_END_DATE) + 1, 0) * 365, 0) as acv,
        opportunity_ext.MIN_START_DATE::date                                                                  as start_date,
        opportunity_ext.MAX_END_DATE::date                                                                    as end_date,
        DATEDIFF('day', MIN_START_DATE, MAX_END_DATE) + 1                                     as length,
        max(mql_reason) as mql_reason
    from final_lead_details
        left join {{ ref('lead') }} on lead.sfid = final_lead_details.SFID
        left join lead_credit on lead_sfid = final_lead_details.SFID
        left join {{ ref('opportunity') }} on lead_credit.opportunity_sfid = opportunity.SFID
        left join{{ ref('opportunity_ext') }} on OPPORTUNITY.SFID = OPPORTUNITY_EXT.OPPORTUNITY_SFID
        left join {{ ref('account') }} on opportunity.accountid = ACCOUNT.sfid
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18 , 19, 20
    order by OPPORTUNITY_SFID asc
), opportunity_hist as (
    select lead_credit.opportunity_sfid,
        MAX(CASE WHEN newvalue in ('1. Discovery (Sales Accepted)', '1-Discovery') THEN OPPORTUNITYFIELDHISTORY.CREATEDDATE ELSE NULL END)::date as stage_1,
        MAX(CASE WHEN newvalue in ('2-Development','2. Enterprise Edition Evaluation') THEN OPPORTUNITYFIELDHISTORY.CREATEDDATE ELSE NULL END)::date as stage_2,
        MAX(CASE WHEN newvalue in ('3-Technical Evaluation','3. Solution Proposal') THEN OPPORTUNITYFIELDHISTORY.CREATEDDATE ELSE NULL END)::date as stage_3,
        MAX(CASE WHEN newvalue in ('4-Negotiation and Quotation','4. In Procurement') THEN OPPORTUNITYFIELDHISTORY.CREATEDDATE ELSE NULL END)::date as stage_4,
        MAX(CASE WHEN newvalue in ('5. Submitted for Booking','5. Submitted to Booking','Submitted to Booking') THEN OPPORTUNITYFIELDHISTORY.CREATEDDATE ELSE NULL END)::date as stage_5,
        MAX(CASE WHEN newvalue in ('5.5 Pending CTA/Payment') THEN OPPORTUNITYFIELDHISTORY.CREATEDDATE ELSE NULL END)::date as stage_55,
        MAX(CASE WHEN newvalue in ('6. Closed Won','Closed Won') THEN OPPORTUNITYFIELDHISTORY.CREATEDDATE ELSE NULL END)::date as stage_6,
        MAX(CASE WHEN newvalue in ('7. Closed Lost','Closed Lost') THEN OPPORTUNITYFIELDHISTORY.CREATEDDATE ELSE NULL END)::date as stage_7
    from lead_credit
    left join {{ ref('opportunityfieldhistory') }} on opportunityfieldhistory.OPPORTUNITYID = opportunity_sfid and field = 'StageName'
    group by 1
)

select 
    opportunity_info.*,
    opportunity_hist.stage_1,
    opportunity_hist.stage_2, 
    opportunity_hist.stage_3, 
    opportunity_hist.stage_4, 
    opportunity_hist.stage_5, 
    opportunity_hist.stage_55, 
    opportunity_hist.stage_6, 
    opportunity_hist.stage_7 
from opportunity_info
left join opportunity_hist on opportunity_hist.opportunity_sfid = opportunity_info.opportunity_sfid
