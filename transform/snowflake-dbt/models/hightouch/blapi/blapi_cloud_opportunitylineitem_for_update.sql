{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

select *
from {{ ref('blapi_cloud_opportunitylineitem') }}
where opportunitylineitem_sfid is not null