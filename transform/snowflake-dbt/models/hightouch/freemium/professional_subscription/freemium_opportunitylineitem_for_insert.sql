{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags": ["hourly", "blapi", "deprecated"]
  })
}}

select *
from {{ ref('freemium_opportunitylineitem') }}
where opportunitylineitem_sfid is null