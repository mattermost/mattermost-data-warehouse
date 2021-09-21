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

web_traffic as (
    select
        date_trunc(month,daily_website_traffic.timestamp) as month,
        count(distinct daily_website_traffic.anonymous_id) as count_web_traffic
    from {{ ref('daily_website_traffic') }}
    left join web.user_agent_registry  AS user_agent_registry ON coalesce(daily_website_traffic.context_useragent, daily_website_traffic.context_user_agent) = user_agent_registry.context_useragent
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
    select date_trunc(month,createddate) as month, count(*) as count_lead_creation
    from {{ ref('lead') }}
    group by 1
),

mql as (
    select date_trunc(month,most_recent_mql_date__c) as month, count(*) as count_mql
    from {{ ref('lead') }}
    group by 1
),

sal as (
    select date_trunc(month,sal_date__c) as month, count(*) as count_sal
    from {{ ref('lead') }}
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
        count(distinct opportunity.sfid) as count_created,
        round(sum(case when created_by_segment = 'Federal' then opportunity_snapshot.amount else 0 end),2) as federal_pipeline,
        round(sum(case when created_by_segment like 'Commercial%' then opportunity_snapshot.amount else 0 end),2) as commercial_ae_pipeline,
        round(sum(case when created_by_segment like 'Enterprise%' then opportunity_snapshot.amount else 0 end),2) as enterprise_ae_pipeline,
        round(sum(case when created_by_segment = 'Commercial BDR' then opportunity_snapshot.amount else 0 end),2) as commercial_bdr_pipeline,
        round(sum(case when created_by_segment = 'Enterprise BDR' then opportunity_snapshot.amount else 0 end),2) as enterprise_bdr_pipeline,
        count(distinct case when created_by_segment = 'Federal' then opportunity.sfid else null end) as federal_created,
        count(distinct case when created_by_segment like 'Commercial%' then opportunity.sfid else null end) as commercial_ae_created,
        count(distinct case when created_by_segment like 'Enterprise%' then opportunity.sfid else null end) as enterprise_ae_created,
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
        count(*) as count_new_logo,
        count(distinct case when market_segment = 'Federal' then opportunity_ext.opportunity_sfid else null end) as federal_new_logo,
        count(distinct case when market_segment = 'Commercial' then opportunity_ext.opportunity_sfid else null end) as commercial_new_logo,
        count(distinct case when market_segment = 'Enterprise' then opportunity_ext.opportunity_sfid else null end) as enterprise_new_logo,
        1.00*count(distinct case when market_segment = 'Federal' then opportunity_ext.opportunity_sfid else null end)/count(*) as perc_federal_new_logo,
        1.00*count(distinct case when market_segment = 'Commercial' then opportunity_ext.opportunity_sfid else null end)/count(*) as perc_commercial_new_logo,
        1.00*count(distinct case when market_segment = 'Enterprise' then opportunity_ext.opportunity_sfid else null end)/count(*) as perc_enterprise_new_logo,
        round(sum(amount),2) as arr,
        round(sum(case when market_segment = 'Federal' then amount else 0 end),2) as federal_arr,
        round(sum(case when market_segment = 'Commercial' then amount else 0 end),2) as commercial_arr,
        round(sum(case when market_segment = 'Enterprise' then amount else 0 end),2) as enterprise_arr,
        round(avg(case when market_segment = 'Federal' then amount else 0 end),2) as federal_avg_amount,
        round(avg(case when market_segment = 'Commercial' then amount else 0 end),2) as commercial_avg_amount,
        round(avg(case when market_segment = 'Enterprise' then amount else 0 end),2) as enterprise_avg_amount,
        round(avg(case when market_segment = 'Federal' then age else 0 end),2) as federal_avg_age,
        round(avg(case when market_segment = 'Commercial' then age else 0 end),2) as commercial_avg_age,
        round(avg(case when market_segment = 'Enterprise' then age else 0 end),2) as enterprise_avg_age
    from {{ ref('opportunity') }}
    join {{ ref('opportunity_ext') }} on opportunity.sfid = opportunity_ext.opportunity_sfid
    where type in ('New Business','New Subscription')
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
        coalesce(sal.count_sal,0) as count_sal,
        coalesce(sql.count_sql,0) as count_sql,
        coalesce(pipeline_created.pipeline_amount,0) as pipeline_amount,
        coalesce(pipeline_created.federal_pipeline,0) as federal_pipeline_amount,
        coalesce(pipeline_created.commercial_ae_pipeline,0) as commercial_ae_pipeline_amount,
        coalesce(pipeline_created.enterprise_ae_pipeline,0) as enterprise_ae_pipeline_amount,
        coalesce(pipeline_created.commercial_bdr_pipeline,0) as commercial_bdr_pipeline_amount,
        coalesce(pipeline_created.enterprise_bdr_pipeline,0) as enterprise_bdr_pipeline_amount,
        coalesce(pipeline_created.count_created,0) as count_creation,
        coalesce(pipeline_created.federal_created,0) as federal_created,
        coalesce(pipeline_created.commercial_ae_created,0) as commercial_ae_created,
        coalesce(pipeline_created.enterprise_ae_created,0) as enterprise_ae_created,
        coalesce(pipeline_created.commercial_bdr_created,0) as commercial_bdr_created,
        coalesce(pipeline_created.enterprise_bdr_created,0) as enterprise_bdr_created,
        coalesce(count_new_logo,0) as count_new_logo,
        coalesce(federal_new_logo,0) as federal_new_logo,
        coalesce(commercial_new_logo,0) as commercial_new_logo,
        coalesce(enterprise_new_logo,0) as enterprise_new_logo,
        coalesce(perc_federal_new_logo,0) as perc_federal_new_logo,
        coalesce(perc_commercial_new_logo,0) as perc_commercial_new_logo,
        coalesce(perc_enterprise_new_logo,0) as perc_enterprise_new_logo,
        coalesce(arr,0) as arr,
        coalesce(federal_arr,0) as federal_arr,
        coalesce(commercial_arr,0) as commercial_arr,
        coalesce(enterprise_arr,0) as enterprise_arr,
        coalesce(federal_avg_amount,0) as federal_avg_amount,
        coalesce(commercial_avg_amount,0) as commercial_avg_amount,
        coalesce(enterprise_avg_amount,0) as enterprise_avg_amount,
        coalesce(federal_avg_age,0) as federal_avg_age,
        coalesce(commercial_avg_age,0) as commercial_avg_age,
        coalesce(enterprise_avg_age,0) as enterprise_avg_age,
        coalesce(marketing_spend.marketing_spend,0) as marketing_spend
    from months
    left join web_traffic on months.month = web_traffic.month
    left join lead_creation on months.month = lead_creation.month
    left join mql on months.month = mql.month
    left join sal on months.month = sal.month
    left join sql on months.month = sql.month
    left join pipeline_created on months.month = pipeline_created.month
    left join new_logo_and_arr_creation on months.month = new_logo_and_arr_creation.month
    left join marketing_spend on months.month = marketing_spend.month
)

select * from marketing_funnel