{{config({    
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

select *
from {{ ref('freemium_opportunitylineitem') }}
where opportunitylineitem_sfid is null