{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}
-- DO NOT DEPLOY
WITH leads_with_freemium_subs as (
    select email,
    last_name,
    domain as company
    FROM {{ ref('customers_with_freemium_subs') }}
)

select * from leads_with_freemium_subs