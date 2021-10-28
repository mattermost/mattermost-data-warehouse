{{
  config({
    "materialized": 'table',
    "schema": "sales"
  })
}}

with months as (
    select date_trunc(month,date_day) as month
    from {{ ref('dates') }}
    where date_trunc(month,date_day) <= util.FISCAL_YEAR_END(util.FISCAL_YEAR(current_date))
    group by 1
),

key_assumptions as (
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
),

web_traffic as (
    select
        date_trunc(month,daily_website_traffic.timestamp) as month,
        count(distinct daily_website_traffic.anonymous_id) as count_web_traffic
    from {{ ref('daily_website_traffic') }}
    left join {{ source('user_agent_registry', 'user_agent_registry') }} AS user_agent_registry ON coalesce(daily_website_traffic.context_useragent, daily_website_traffic.context_user_agent) = user_agent_registry.context_useragent
    where split_part(
        regexp_substr(
            case
                when nullif(split_part(context_page_url, '?', 2),'') is not null
                    then case
                        when regexp_substr(split_part(context_page_url, '?', 1), 'preparing[-]{1}workspace') = 'preparing-workspace'
                            then 'https://customers.mattermost.com/preparing-workspace'
                        else split_part(context_page_url, '?', 1)
                    end
                when regexp_substr(context_page_url, 'preparing[-]{1}workspace') = 'preparing-workspace'
                    then 'https://customers.mattermost.com/preparing-workspace'
                when nullif(split_part(context_page_url, '/requests/', 2),'') is not null
                    then split_part(context_page_url, '/requests/', 1) || '/requests'
                else context_page_url
            end,'^(https://|http://)([a-z0-9-]{1,20}[\.]{1}|[A-Za-z0-9-]{1,100})[A-Za-z0-9-]{0,100}[\.]{1}[a-z]{1,10}'
        ),'//', 2) in ('mattermost.com','docs.mattermost.com','integrations.mattermost.com')
        and (user_agent_registry.device_type != 'Spider' or user_agent_registry.device_type is null)
    group by 1
),

lead_creation as (
    select date_trunc(month,createddate) as month, count(distinct lower(email)) as count_lead_creation
    from {{ ref('lead') }}
    where status != 'Not a Lead' and lead_source_text__c != 'Sales Generated'
        and email not like '%@mattermost.com' and email not like '%+%' and email != '' and email not like '%test%'
        and company not like '%example%' and company not like '%ЛУЧШИЙ СПОСОБ ЗАРАБОТКА http://one33.ru/%' and company not like '%QQ%'
        and company not like '%Tencent%' and company not like '%Sina%' and company not like '%NetEase%' and company not like '%Net Ease%'
    group by 1
),

mql as (
    select date_trunc(month,first_mql_date__c) as month, count(distinct lower(email)) as count_mql
    from {{ ref('lead') }}
    where status != 'Not a Lead' and lead_source_text__c != 'Sales Generated'
        and email not like '%@mattermost.com' and email not like '%+%' and email != '' and email not like '%test%'
        and company not like '%example%' and company not like '%ЛУЧШИЙ СПОСОБ ЗАРАБОТКА http://one33.ru/%' and company not like '%QQ%'
        and company not like '%Tencent%' and company not like '%Sina%' and company not like '%NetEase%' and company not like '%Net Ease%'
    group by 1
),

reengaged_mql as (
    select date_trunc(month,most_recent_mql_date__c) as month, count(distinct lower(email)) as count_reengaged_mql
    from {{ ref('lead') }}
    where most_recent_mql_date__c is not null and most_recent_reengaged_date__c is not null
        and status != 'Not a Lead' and lead_source_text__c != 'Sales Generated'
        and email not like '%@mattermost.com' and email not like '%+%' and email != '' and email not like '%test%'
        and company not like '%example%' and company not like '%ЛУЧШИЙ СПОСОБ ЗАРАБОТКА http://one33.ru/%' and company not like '%QQ%'
        and company not like '%Tencent%' and company not like '%Sina%' and company not like '%NetEase%' and company not like '%Net Ease%'
    group by 1
),

sal as (
    select date_trunc(month,sal_date__c) as month, count(distinct lower(email)) as count_sal
    from {{ ref('lead') }}
    where status != 'Not a Lead' and lead_source_text__c != 'Sales Generated'
        and email not like '%@mattermost.com' and email not like '%+%' and email != '' and email not like '%test%'
        and company not like '%example%' and company not like '%ЛУЧШИЙ СПОСОБ ЗАРАБОТКА http://one33.ru/%' and company not like '%QQ%'
        and company not like '%Tencent%' and company not like '%Sina%' and company not like '%NetEase%' and company not like '%Net Ease%'
    group by 1
),

sql as (
    select date_trunc(month,activitydate) as month, count(*) as count_sql
    from {{ ref('event') }}
    where type = 'Discovery'
    group by 1
),

pipeline_created as (
    select
        date_trunc(month,opportunity.createddate) as month,
        round(sum(opportunity_snapshot.amount),2) as pipeline_amount,
        count(distinct opportunity.accountid) as count_created,
        round(sum(case when created_by_segment like 'Federal%' then opportunity_snapshot.amount else 0 end),2) as federal_pipeline,
        round(sum(case when created_by_segment like 'Commercial%' then opportunity_snapshot.amount else 0 end),2) as commercial_ae_pipeline,
        round(sum(case when created_by_segment like 'Enterprise%' then opportunity_snapshot.amount else 0 end),2) as enterprise_ae_pipeline,
        round(sum(case when created_by_segment = 'Federal BDR' then opportunity_snapshot.amount else 0 end),2) as federal_bdr_pipeline,
        round(sum(case when created_by_segment = 'Commercial BDR' then opportunity_snapshot.amount else 0 end),2) as commercial_bdr_pipeline,
        round(sum(case when created_by_segment = 'Enterprise BDR' then opportunity_snapshot.amount else 0 end),2) as enterprise_bdr_pipeline,
        count(distinct case when created_by_segment like 'Federal%' then opportunity.sfid else null end) as federal_created,
        count(distinct case when created_by_segment like 'Commercial%' then opportunity.sfid else null end) as commercial_ae_created,
        count(distinct case when created_by_segment like 'Enterprise%' then opportunity.sfid else null end) as enterprise_ae_created,
        count(distinct case when created_by_segment = 'Federal BDR' then opportunity.sfid else null end) as federal_bdr_created,
        count(distinct case when created_by_segment = 'Commercial BDR' then opportunity.sfid else null end) as commercial_bdr_created,
        count(distinct case when created_by_segment = 'Enterprise BDR' then opportunity.sfid else null end) as enterprise_bdr_created
    from {{ ref('opportunity') }}
    join {{ ref('opportunity_ext') }} on opportunity.sfid = opportunity_ext.opportunity_sfid
    left join {{ ref('opportunity_snapshot') }} on opportunity.sfid = opportunity_snapshot.sfid
        and opportunity_snapshot.snapshot_date = coalesce(date_trunc(month,opportunity.createddate) + interval '1 month' - interval '1 day', current_date)
    where opportunity.type in ('New Business','New Subscription')
    group by 1
),

new_logo_and_arr_creation as (
    select 
        date_trunc(month,opportunity.closedate) as month, 
        count(distinct opportunity.sfid) as count_new_logo,
        count(distinct case when market_segment = 'Federal' then opportunity_ext.opportunity_sfid else null end) as federal_new_logo,
        count(distinct case when market_segment = 'Commercial' then opportunity_ext.opportunity_sfid else null end) as commercial_new_logo,
        count(distinct case when market_segment = 'Enterprise' then opportunity_ext.opportunity_sfid else null end) as enterprise_new_logo,
        count(distinct case when created_by_segment = 'Federal BDR' then opportunity_ext.opportunity_sfid else null end) as federal_bdr_new_logo,
        count(distinct case when created_by_segment = 'Commercial BDR' then opportunity_ext.opportunity_sfid else null end) as commercial_bdr_new_logo,
        count(distinct case when created_by_segment = 'Enterprise BDR' then opportunity_ext.opportunity_sfid else null end) as enterprise_bdr_new_logo,
        round(sum(net_new_arr_with_override__c),2) as arr,
        round(sum(case when market_segment = 'Federal' then net_new_arr_with_override__c else 0 end),2) as federal_arr,
        round(sum(case when market_segment = 'Commercial' then net_new_arr_with_override__c else 0 end),2) as commercial_arr,
        round(sum(case when market_segment = 'Enterprise' then net_new_arr_with_override__c else 0 end),2) as enterprise_arr,
        round(sum(case when created_by_segment = 'Federal BDR' then net_new_arr_with_override__c else 0 end),2) as federal_bdr_arr,
        round(sum(case when created_by_segment = 'Commercial BDR' then net_new_arr_with_override__c else 0 end),2) as commercial_bdr_arr,
        round(sum(case when created_by_segment = 'Enterprise BDR' then net_new_arr_with_override__c else 0 end),2) as enterprise_bdr_arr,
        round(sum(case when market_segment = 'Federal' then age else 0 end),2) as federal_sum_age,
        round(sum(case when market_segment = 'Commercial' then age else 0 end),2) as commercial_sum_age,
        round(sum(case when market_segment = 'Enterprise' then age else 0 end),2) as enterprise_sum_age
    from {{ ref('opportunity') }}
    join {{ ref('opportunity_ext') }} on opportunity.sfid = opportunity_ext.opportunity_sfid
    where opportunity.type in ('New Business','New Subscription')
        and opportunity.iswon
    group by 1
),

m18 as (
    select
        date_trunc(month,opportunity.closedate) + interval '18 months' as month,
        round(sum(case when market_segment = 'Federal' then total_arr else 0 end),2) as federal_m18_arr,
        round(sum(case when market_segment = 'Commercial' then total_arr else 0 end),2) as commercial_m18_arr,
        round(sum(case when market_segment = 'Enterprise' then total_arr else 0 end),2) as enterprise_m18_arr
    from {{ ref('opportunity') }}
    join {{ ref('opportunity_ext') }} on opportunity.sfid = opportunity_ext.opportunity_sfid
    left join {{ ref('account_daily_arr') }} on opportunity.accountid = account_daily_arr.account_sfid and date_trunc(month,opportunity.closedate) + interval '18 months' = account_daily_arr.day
    where opportunity.type in ('New Business','New Subscription')
        and opportunity.iswon
    group by 1
),

marketing_spend as (
    select month, spend as marketing_spend
    from {{ source('marketing_gsheets','fy22_budget_by_month') }}
),

marketing_spend as (
    select month, spend as marketing_spend
    from {{ source('marketing_gsheets','fy22_budget_by_month') }}
),

marketing_funnel as (
    select
        months.month,
        coalesce(web_traffic.count_web_traffic,0) as count_web_traffic,
        coalesce(lead_creation.count_lead_creation,0) as count_lead_creation,
        coalesce(mql.count_mql,0) as count_mql,
        coalesce(reengaged_mql.count_reengaged_mql,0) as count_reengaged_mql,
        coalesce(sal.count_sal,0) as count_sal,
        coalesce(sql.count_sql,0) as count_sql,
        coalesce(pipeline_created.pipeline_amount,0) as pipeline_amount,
        coalesce(pipeline_created.federal_pipeline,0) as federal_pipeline_amount,
        coalesce(pipeline_created.commercial_ae_pipeline,0) as commercial_ae_pipeline_amount,
        coalesce(pipeline_created.enterprise_ae_pipeline,0) as enterprise_ae_pipeline_amount,
        coalesce(pipeline_created.federal_bdr_pipeline,0) as federal_bdr_pipeline_amount,
        coalesce(pipeline_created.commercial_bdr_pipeline,0) as commercial_bdr_pipeline_amount,
        coalesce(pipeline_created.enterprise_bdr_pipeline,0) as enterprise_bdr_pipeline_amount,
        coalesce(pipeline_created.count_created,0) as count_creation,
        coalesce(pipeline_created.federal_created,0) as federal_created,
        coalesce(pipeline_created.commercial_ae_created,0) as commercial_ae_created,
        coalesce(pipeline_created.enterprise_ae_created,0) as enterprise_ae_created,
        coalesce(pipeline_created.federal_bdr_created,0) as federal_bdr_created,
        coalesce(pipeline_created.commercial_bdr_created,0) as commercial_bdr_created,
        coalesce(pipeline_created.enterprise_bdr_created,0) as enterprise_bdr_created,
        coalesce(count_new_logo,0) as count_new_logo,
        coalesce(federal_new_logo,0) as federal_new_logo,
        coalesce(commercial_new_logo,0) as commercial_new_logo,
        coalesce(enterprise_new_logo,0) as enterprise_new_logo,
        coalesce(federal_bdr_new_logo,0) as federal_bdr_new_logo,
        coalesce(commercial_bdr_new_logo,0) as commercial_bdr_new_logo,
        coalesce(enterprise_bdr_new_logo,0) as enterprise_bdr_new_logo,
        coalesce(arr,0) as arr,
        coalesce(federal_arr,0) as federal_arr,
        coalesce(commercial_arr,0) as commercial_arr,
        coalesce(enterprise_arr,0) as enterprise_arr,
        coalesce(federal_bdr_arr,0) as federal_bdr_arr,
        coalesce(commercial_bdr_arr,0) as commercial_bdr_arr,
        coalesce(enterprise_bdr_arr,0) as enterprise_bdr_arr,
        coalesce(federal_sum_age,0) as federal_sum_age,
        coalesce(commercial_sum_age,0) as commercial_sum_age,
        coalesce(enterprise_sum_age,0) as enterprise_sum_age,
        coalesce(federal_m18_arr,0) as federal_m18_arr,
        coalesce(commercial_m18_arr,0) as commercial_m18_arr,
        coalesce(enterprise_m18_arr,0) as enterprise_m18_arr,
        coalesce(marketing_spend.marketing_spend,0) as marketing_spend,
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
    from months
    left join key_assumptions on months.month = key_assumptions.month
    left join web_traffic on months.month = web_traffic.month
    left join lead_creation on months.month = lead_creation.month
    left join mql on months.month = mql.month
    left join reengaged_mql on months.month = reengaged_mql.month
    left join sal on months.month = sal.month
    left join sql on months.month = sql.month
    left join pipeline_created on months.month = pipeline_created.month
    left join new_logo_and_arr_creation on months.month = new_logo_and_arr_creation.month
    left join m18 on months.month = m18.month
    left join marketing_spend on months.month = marketing_spend.month
)

select * from marketing_funnel