{{
    config({
    "materialized": 'table',
    "schema": "hightouch"
    })
}}


with recent_trial_requests as (
    select *
    from {{ ref('trial_requests') }}
    where license_issued_at > current_date - interval '1 day'
)