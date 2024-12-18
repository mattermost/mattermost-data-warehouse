WITH opportunity_daily_arr AS (
  SELECT
    account.account_id,
    coalesce(master_account.account_id, account.account_id) as master_account_sfid,
    opportunity.opportunity_id,
  	day,
  	SUM(won_arr)::int AS won_arr,
    SUM(lost_arr)::int AS lost_arr,
    SUM(open_arr)::int AS open_arr
  FROM {{ ref( 'int_opportunity_line_item_daily_arr') }}  AS opportunitylineitem
  LEFT JOIN {{ ref( 'stg_salesforce__opportunity') }} AS opportunity ON opportunity.opportunity_id = opportunitylineitem.opportunity_id
  LEFT JOIN {{ ref( 'stg_salesforce__account') }}  AS account ON account.account_id = opportunity.account_id
  LEFT JOIN {{ ref( 'stg_salesforce__account') }}  AS master_account ON master_account.account_id = account.parent_id
  GROUP BY 1, 2, 3, 4
)
SELECT * FROM opportunity_daily_arr