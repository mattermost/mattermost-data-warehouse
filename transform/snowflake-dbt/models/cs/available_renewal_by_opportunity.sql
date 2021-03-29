{{config({
    "materialized": "table",
    "schema": "cs"
  })
}}

select account_id, available_renewal, renewal_license_start, renewal_opportunity, renewal_qtr from {{ source('sales_and_cs_gsheets','available_renewals_current_fy') }}
union all 
select account_id, available_renewal, renewal_license_start, renewal_opportunity, renewal_qtr from {{ source('sales_and_cs_gsheets','available_renewals_historical') }}