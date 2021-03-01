{{config({
    "materialized": "table",
    "schema": "cs"
  })
}}

select * from {{ source('sales_and_cs_gsheets','available_renewals_current_fy') }}
union all 
select * from {{ source('sales_and_cs_gsheets','available_renewals_historical') }}