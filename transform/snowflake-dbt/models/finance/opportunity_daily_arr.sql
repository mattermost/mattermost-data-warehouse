{{
  config({
    "materialized": "table",
  })
}}

WITH opportunity_daily_arr AS (
  SELECT
    account.sfid as account_sfid,
    coalesce(master_account.sfid, account.sfid) as master_account_sfid,
    opportunity.sfid AS opportunity_sfid,
  	day,
  	SUM(won_arr)::int AS won_arr,
    SUM(lost_arr)::int AS lost_arr,
    SUM(open_arr)::int AS open_arr
  FROM {{ ref( 'opportunitylineitem_daily_arr') }}  AS opportunitylineitem
  LEFT JOIN {{ ref( 'opportunity') }}  AS opportunity ON opportunity.sfid = opportunitylineitem.opportunity_sfid
  LEFT JOIN {{ ref( 'account') }}  AS account ON account.sfid = opportunity.accountid
  LEFT JOIN {{ ref( 'account') }}  AS master_account ON master_account.sfid = account.parentid
  GROUP BY 1, 2, 3, 4
)
SELECT * FROM opportunity_daily_arr