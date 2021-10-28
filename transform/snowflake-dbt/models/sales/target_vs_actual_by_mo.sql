{{
  config({
    "materialized": 'table',
    "schema": "sales"
  })
}}

with months as (
    select 
        month, 
        extract(day from last_day(month::date)) as days_in_period,
        case when month = date_trunc(month, current_date) then extract(day from current_date) - 1 when last_day(month::date) < current_date then extract(day from last_day(month::date)) else 0 end as days_completed_in_period,
        case when month = date_trunc(month, current_date) then datediff(day,current_date,last_day(month::date)) + 1 when last_day(month::date) < current_date then 0 else extract(day from last_day(month::date)) end as days_left_in_period,
        1.00*(case when month = date_trunc(month, current_date) then extract(day from current_date) - 1 when last_day(month::date) < current_date then extract(day from last_day(month::date)) else 0 end) / extract(day from last_day(month::date)) as percent_completed
    from {{ source('marketing_gsheets','fy22_bottoms_up_targets') }}
), targets_prorated as (
    select 
        fy22_bottoms_up_targets.month,
        months.days_in_period,
        months.days_completed_in_period,
        months.days_left_in_period,
        months.percent_completed,
        fy22_bottoms_up_targets.leads as leads_target,
        round(fy22_bottoms_up_targets.leads * percent_completed,0) as leads_target_prorated,
        fy22_bottoms_up_targets.mqls as mqls_target, 
        round(fy22_bottoms_up_targets.mqls * percent_completed,0) as mqls_target_prorated,
        fy22_bottoms_up_targets.sals as sals_target,
        round(fy22_bottoms_up_targets.sals * percent_completed,0) as sals_target_prorated,
        fy22_bottoms_up_targets.sqls as sqls_target,
        round(fy22_bottoms_up_targets.sqls * percent_completed,0) as sqls_target_prorated,
        fy22_bottoms_up_targets.opportunities as opportunities_target,
        round(fy22_bottoms_up_targets.opportunities * percent_completed,0) as opportunities_target_prorated,
        fy22_bottoms_up_targets.new_logos as new_logos_target,
        round(fy22_bottoms_up_targets.new_logos * percent_completed,0) as new_logos_target_prorated,
        fy22_bottoms_up_targets.new_pipeline as new_pipeline_target,
        round(fy22_bottoms_up_targets.new_pipeline * percent_completed,0) as new_pipeline_target_prorated,
        fy22_bottoms_up_targets.new_arr as new_arr_target,
        round(fy22_bottoms_up_targets.new_arr * percent_completed,0) as new_arr_target_prorated
    from {{ source('marketing_gsheets','fy22_bottoms_up_targets') }}
    join months on fy22_bottoms_up_targets.month = months.month
), key_assumptions as (
    select
        month,
        run_rate,
        budget,
        cpl,
        traffic_lead,
        lead_mql,
        mql_sal,
        sal_sql,
        sql_opp,
        opp_newlogo,
        lead_mql_coverage,
        mql_sal_coverage,
        sal_sql_coverage,
        sql_opp_coverage,
        opp_logo_coverage,
        pipeline_arr_coverage,
        avg_quota_achievement,
        percent_fed_logos,
        percent_comm_logos,
        percent_ent_logos,
        asp_fed,
        asp_comm,
        asp_ent,
        avg_sales_cyle_fed,
        avg_sales_cycle_comm,
        avg_sales_cycle_ent,
        fed_expansion,
        comm_expansion,
        ent_expansion,
        comm_bdr_quota_opps,
        comm_bdr_quota_pipeline,
        comm_bdr_logos,
        comm_bdr_new_arr,
        ent_fed_bdr_quota_opps,
        ent_fed_bdr_quota_pipeline,
        fed_ae_quota_opps,
        fed_ae_quota_pipeline,
        fed_ae_quota_logos,
        fed_ae_quota_arr,
        comm_ae_quota_opps,
        comm_ae_quota_pipeline,
        comm_ae_logos,
        comm_ae_new_arr,
        ent_ae_quota_opps,
        ent_ae_quota_pipeline,
        ent_ae_logos,
        ent_ae_new_arr
    from {{ source('marketing_gsheets','fy22_key_assumptions') }}
), leads as (
    select date_trunc(month, createddate) as month, count(*) as leads_actual
    from {{ ref('lead') }}
    where util.fiscal_year(createddate) = util.fiscal_year(current_date)
    group by 1
), mqls as (
    select date_trunc(month, most_recent_mql_date__c) as month, count(*) as mqls_actual
    from {{ ref('lead') }}
    where util.fiscal_year(most_recent_mql_date__c) = util.fiscal_year(current_date)
    group by 1
), sals as (
    select date_trunc(month, most_recent_scl_date__c) as month, count(*) as sals_actual
    from {{ ref('lead') }}
    where util.fiscal_year(most_recent_scl_date__c) = util.fiscal_year(current_date)
    group by 1
), sqls as (
    select date_trunc(month, most_recent_qsc_date__c) as month, count(*) as sqls_actual
    from {{ ref('lead') }}
    where util.fiscal_year(most_recent_qsc_date__c) = util.fiscal_year(current_date)
    group by 1
), opportunities as (
    select date_trunc(month, createddate) as month, count(*) as opportunities_actual
    from {{ ref('opportunity') }}
    where util.fiscal_year(createddate) = util.fiscal_year(current_date)
        and type = 'New Subscription'
    group by 1
), new_logos as (
    select date_trunc(month, createddate) as month, count(*) as new_logos_actual
    from {{ ref('opportunity') }}
    where util.fiscal_year(createddate) = util.fiscal_year(current_date)
        and new_logo__c
    group by 1
), new_pipeline as (
    select date_trunc(month, createddate) as month, sum(amount) as new_pipeline_actual
    from {{ ref('opportunity') }}
    where util.fiscal_year(createddate) = util.fiscal_year(current_date)
        and type = 'New Subscription'
    group by 1
), new_arr as (
    select date_trunc(month, createddate) as month, sum(amount) as new_arr_actual
    from {{ ref('opportunity') }}
    where util.fiscal_year(createddate) = util.fiscal_year(current_date)
        and type = 'New Subscription'
        and iswon
    group by 1
), target_vs_actual_by_mo as (
    select 
        targets_prorated.month,
        leads_target,
        leads_target_prorated,
        leads_actual,
        round(leads_actual/nullif(days_completed_in_period,0) * days_in_period,0) as leads_projected,
        leads_actual/nullif(leads_target_prorated,0) as leads_tva_prorated,
        leads_actual/nullif(leads_target,0) as leads_tva_actual,
        mqls_target,
        mqls_target_prorated,
        mqls_actual,
        round(mqls_actual/nullif(days_completed_in_period,0) * days_in_period,0) as mqls_projected,
        mqls_actual/nullif(mqls_target_prorated,0) as mqls_tva_prorated,
        mqls_actual/nullif(mqls_target,0) as mqls_tva_actual,
        sals_target,
        sals_target_prorated,
        sals_actual,
        round(sals_actual/nullif(days_completed_in_period,0) * days_in_period,0) as sals_projected,
        sals_actual/nullif(sals_target_prorated,0) as sals_tva_prorated,
        sals_actual/nullif(sals_target,0) as sals_tva_actual,
        sqls_target,
        sqls_target_prorated,
        sqls_actual,
        round(sqls_actual/nullif(days_completed_in_period,0) * days_in_period,0) as sqls_projected,
        sqls_actual/nullif(sqls_target_prorated,0) as sqls_tva_prorated,
        sqls_actual/nullif(sqls_target,0) as sqls_tva_actual,
        opportunities_target,
        opportunities_target_prorated,
        opportunities_actual,
        round(opportunities_actual/nullif(days_completed_in_period,0) * days_in_period,0) as opportunities_projected,
        opportunities_actual/nullif(opportunities_target_prorated,0) as opportunities_tva_prorated,
        opportunities_actual/nullif(opportunities_target,0) as opportunities_tva_actual,
        new_logos_target,
        new_logos_target_prorated,
        new_logos_actual,
        round(new_logos_actual/nullif(days_completed_in_period,0) * days_in_period,0) as new_logos_projected,
        new_logos_actual/nullif(new_logos_target_prorated,0) as new_logos_tva_prorated,
        new_logos_actual/nullif(new_logos_target,0) as new_logos_tva_actual,
        new_pipeline_target,
        new_pipeline_target_prorated,
        new_pipeline_actual,
        round(new_pipeline_actual/nullif(days_completed_in_period,0) * days_in_period,0) as new_pipeline_projected,
        new_pipeline_actual/nullif(new_pipeline_target_prorated,0) as new_pipeline_tva_prorated,
        new_pipeline_actual/nullif(new_pipeline_target,0) as new_pipeline_tva_actual,
        new_arr_target,
        new_arr_target_prorated,
        new_arr_actual,
        round(new_arr_actual/nullif(days_completed_in_period,0) * days_in_period,0) as new_arr_projected,
        new_arr_actual/nullif(new_arr_target_prorated,0) as new_arr_tva_prorated,
        new_arr_actual/nullif(new_arr_target,0) as new_arr_tva_actual,
        key_assumptions.run_rate as run_rate_assumption,
        key_assumptions.budget as budget_assumption,
        key_assumptions.cpl as cpl_assumption,
        key_assumptions.traffic_lead as traffic_lead_assumption,
        key_assumptions.lead_mql as lead_mql_assumption,
        key_assumptions.mql_sal as mql_sal_assumption,
        key_assumptions.sal_sql as sal_sql_assumption,
        key_assumptions.sql_opp as sql_opp_assumption,
        key_assumptions.opp_newlogo as opp_newlogo_assumption,
        key_assumptions.lead_mql_coverage as lead_mql_coverage_assumption,
        key_assumptions.mql_sal_coverage as mql_sal_coverage_assumption,
        key_assumptions.sal_sql_coverage as sal_sql_coverage_assumption,
        key_assumptions.sql_opp_coverage as sql_opp_coverage_assumption,
        key_assumptions.opp_logo_coverage as opp_logo_coverage_assumption,
        key_assumptions.pipeline_arr_coverage as pipeline_arr_coverage_assumption,
        key_assumptions.avg_quota_achievement as avg_quota_achievement_assumption,
        key_assumptions.percent_fed_logos as percent_fed_logos_assumption,
        key_assumptions.percent_comm_logos as percent_comm_logos_assumption,
        key_assumptions.percent_ent_logos as percent_ent_logos_assumption,
        key_assumptions.asp_fed as asp_fed_assumption,
        key_assumptions.asp_comm as asp_comm_assumption,
        key_assumptions.asp_ent as asp_ent_assumption,
        key_assumptions.avg_sales_cyle_fed as avg_sales_cyle_fed_assumption,
        key_assumptions.avg_sales_cycle_comm as avg_sales_cycle_comm_assumption,
        key_assumptions.avg_sales_cycle_ent as avg_sales_cycle_ent_assumption,
        key_assumptions.fed_expansion as fed_expansion_assumption,
        key_assumptions.comm_expansion as comm_expansion_assumption,
        key_assumptions.ent_expansion as ent_expansion_assumption,
        key_assumptions.comm_bdr_quota_opps as comm_bdr_quota_opps_assumption,
        key_assumptions.comm_bdr_quota_pipeline as comm_bdr_quota_pipeline_assumption,
        key_assumptions.comm_bdr_logos as comm_bdr_logos_assumption,
        key_assumptions.comm_bdr_new_arr as comm_bdr_new_arr_assumption,
        key_assumptions.ent_fed_bdr_quota_opps as ent_fed_bdr_quota_opps_assumption,
        key_assumptions.ent_fed_bdr_quota_pipeline as ent_fed_bdr_quota_pipeline_assumption,
        key_assumptions.fed_ae_quota_opps as fed_ae_quota_opps_assumption,
        key_assumptions.fed_ae_quota_pipeline as fed_ae_quota_pipeline_assumption,
        key_assumptions.fed_ae_quota_logos as fed_ae_quota_logos_assumption,
        key_assumptions.fed_ae_quota_arr as fed_ae_quota_arr_assumption,
        key_assumptions.comm_ae_quota_opps as comm_ae_quota_opps_assumption,
        key_assumptions.comm_ae_quota_pipeline as comm_ae_quota_pipeline_assumption,
        key_assumptions.comm_ae_logos as comm_ae_logos_assumption,
        key_assumptions.comm_ae_new_arr as comm_ae_new_arr_assumption,
        key_assumptions.ent_ae_quota_opps as ent_ae_quota_opps_assumption,
        key_assumptions.ent_ae_quota_pipeline as ent_ae_quota_pipeline_assumption,
        key_assumptions.ent_ae_logos as ent_ae_logos_assumption,
        key_assumptions.ent_ae_new_arr as ent_ae_new_arr_assumption
    from targets_prorated
    left join key_assumptions on targets_prorated.month = key_assumptions.month
    left join leads on targets_prorated.month = leads.month
    left join mqls on targets_prorated.month = mqls.month
    left join sals on targets_prorated.month = sals.month
    left join sqls on targets_prorated.month = sqls.month
    left join opportunities on targets_prorated.month = opportunities.month
    left join new_logos on targets_prorated.month = new_logos.month
    left join new_pipeline on targets_prorated.month = new_pipeline.month
    left join new_arr on targets_prorated.month = new_arr.month
)

select * from target_vs_actual_by_mo
order by 1 asc