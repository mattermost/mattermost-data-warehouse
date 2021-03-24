{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}
with account_update_health_score as (
    select 
        account.sfid,
        account_health_score.health_score_w_override as health_score
    from {{ ref('account') }}
    left join {{ ref('account_health_score') }} on account.sfid = account_health_score.account_sfid
    where 
        (account.health_score__c)
        is distinct from
        (account_health_score.health_score_w_override)
)

select * from account_update_health_score