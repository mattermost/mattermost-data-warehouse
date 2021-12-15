{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly","blapi"]
  })
}}

select
    sfid,
    left(sfid, 8) || '-'
    || substring(sfid, 9, 4) || '-'
    || substring(sfid, 13, 4) || '-'
    || substring(sfid, 15, 4) || '-'
    || right(sfid, 12) as dwh_external_id__c
from
    {{ ref('lead') }}
where dwh_external_id__c is null