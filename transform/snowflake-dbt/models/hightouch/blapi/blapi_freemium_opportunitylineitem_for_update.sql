{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

select *
from {{ ref('blapi_freemium_opportunitylineitem') }}
where opportunitylineitem_sfid is not null