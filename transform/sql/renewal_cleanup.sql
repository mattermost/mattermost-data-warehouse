WITH ending_opportunities AS (
    SELECT account.name                                                                     AS account_name,
           account.sfid                                                                     AS account_sfid,
           account_owner.NAME                                                               AS account_owner_name,
           account_csm.NAME                                                                 AS account_csm_name,
           opportunity.name                                                                 AS opportunity_name,
           opportunity.sfid                                                                 AS opportunity_sfid,
           opportunity_ext.max_end_date::date                                               AS license_max_end_date,
           date_trunc('month', opportunity_ext.max_end_date)::date                          AS license_max_end_month,
           opportunity_ext.num_diff_end_dates                                               AS num_diff_license_end_dates,
           opportunity.amount                                                               AS opportunity_amount,
           opportunity.license_key__c                                                       AS opportunity_license_key,
           product2.name                                                                    AS oli_product_name,
           opportunitylineitem.unitprice                                                    AS oli_unitprice,
           opportunitylineitem.quantity                                                     AS oli_quantity,
           opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date  AS oli_length_days,
           opportunitylineitem.product_line_type__c                                         AS oli_product_line_type,
           opportunitylineitem.is_prorated_expansion__c                                     AS oli_prorated_expansion
    FROM orgm.account AS account
             LEFT JOIN orgm.opportunity ON account.sfid = opportunity.accountid
             LEFT JOIN orgm.opportunity_ext ON opportunity.sfid = opportunity_ext.opportunity_sfid
             LEFT JOIN orgm.opportunitylineitem ON opportunity.sfid = opportunitylineitem.opportunityid
             LEFT JOIN orgm.product2 ON opportunitylineitem.product2id = product2.sfid
             LEFT JOIN orgm."USER" AS account_csm ON account.csm_lookup__c = account_csm.sfid
             LEFT JOIN orgm."USER" AS account_owner ON account.ownerid = account_owner.sfid
    WHERE opportunity.iswon
        AND opportunity_ext.max_end_date >= '2020-07-01'
        AND opportunitylineitem.end_date__c::date - opportunitylineitem.start_date__c::date <> 0
), ending_opportunity_details as (
select
       account_name,
       account_sfid,
       account_owner_name,
       account_csm_name,
       license_max_end_date,
       license_max_end_month,
       count(distinct opportunity_sfid) as count_opportunities,
       listagg(distinct opportunity_name,',') as opportunity_name,
       listagg(distinct opportunity_sfid,',') as opportunity_sfids,
       sum(opportunity_amount) as opportunity_sum_amount,
       listagg(distinct opportunity_amount,',') as opportunity_amounts,
       listagg(distinct case when opportunity_license_key is null then opportunity_sfid else null end,',') as opportunity_sfids_missing_license,
       listagg(distinct opportunity_license_key,',') as opportunity_license_keys,
       listagg(distinct oli_product_name,',') as oli_product_names,
       listagg(distinct oli_unitprice,',') as oli_unitprices,
       listagg(distinct oli_quantity,',') as oli_quantities,
       listagg(distinct round(365*oli_unitprice/oli_length_days,2),',') as oli_yearly_unit_prices,
       listagg(distinct oli_length_days,',') as oli_length_days,
       listagg(distinct oli_product_line_type,',') as oli_product_line_types,
       sum(distinct case when opportunity_license_key is null then 1 else 0 end) as null_license_keys,
       count(distinct opportunity_license_key) < count(distinct opportunity_sfid) as less_keys_than_opportunities,
       count(distinct opportunity_sfid) > 1 as has_two_plus_oppt_ending,
       count(distinct oli_product_name) > 1 as has_two_plus_product_names,
       listagg(distinct oli_product_name,',') like '%Premier Support%' and listagg(distinct oli_product_name,',') not like '%with Premier Support%' as has_legacy_ps,
       listagg(distinct oli_product_name,',') like '%Legacy%' as has_legacy_sku,
       listagg(distinct oli_product_name,',') like '%NP%' or listagg(distinct oli_product_name,',') like '%Academic%' or listagg(distinct oli_product_name,',') like '%Student%' or listagg(distinct oli_product_name,',') like '%Not%' as weird_sku,
       listagg(distinct oli_prorated_expansion,',') like '%Leftover Expansion%' as has_loe,
       listagg(distinct oli_prorated_expansion,',') like '%Co-Term%' as has_coterm
from ending_opportunities
group by 1, 2, 3, 4, 5, 6
)
select
       ending_opportunity_details.*,
       opportunity.sfid as upcoming_opportunity_sfid,
       opportunity.status_wlo__c as upcoming_opportunity_status_wlo,
       opportunity.amount as upcoming_opportunity_amount,
       opportunity_sum_amount = opportunity.amount as ending_amount_matches_upcoming_amount,
       listagg(distinct opportunitylineitem.product_line_type__c,',') as upcoming_product_line_types,
       listagg(distinct opportunitylineitem.product_line_type__c,',') = 'Ren' as upcoming_ren_only
from ending_opportunity_details
left join orgm.opportunity_ext
    on opportunity_ext.min_start_date - interval '1 day' = ending_opportunity_details.license_max_end_date
        and opportunity_ext.accountid = ending_opportunity_details.account_sfid
left join orgm.opportunity on opportunity.sfid = opportunity_ext.opportunity_sfid
left join orgm.opportunitylineitem on opportunitylineitem.opportunityid = opportunity.sfid
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31