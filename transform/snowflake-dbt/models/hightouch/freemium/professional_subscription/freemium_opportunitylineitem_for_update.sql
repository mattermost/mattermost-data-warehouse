{{config({
    "schema": "hightouch",
    "unique_key":"id",
    "tags": ["hourly", "blapi", "deprecated"]
  })
}}

select *
from {{ ref('freemium_opportunitylineitem') }}
where opportunitylineitem_sfid is not null