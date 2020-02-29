{{config({
    "materialized": 'table',
    "schema": "cs"
  })
}}

WITH zendesk_ticket_details as (
    SELECT
        tickets.id as ticket_id,
        organizations.name as organization_name,
        account.sfid as account_sfid,
        users.name as assignee_name,
        agent_wait_time_in_minutes:business::int agent_wait_time_in_minutes_bus,
        agent_wait_time_in_minutes:calendar::int agent_wait_time_in_minutes_cal,
        first_resolution_time_in_minutes:business::int first_resolution_time_in_minutes_bus,
        first_resolution_time_in_minutes:calendar::int first_resolution_time_in_minutes_cal,
        full_resolution_time_in_minutes:business::int full_resolution_time_in_minutes_bus,
        full_resolution_time_in_minutes:calendar::int full_resolution_time_in_minutes_cal,
        on_hold_time_in_minutes:business::int on_hold_time_in_minutes_bus,
        on_hold_time_in_minutes:calendar::int on_hold_time_in_minutes_cal,
        reply_time_in_minutes:business::int reply_time_in_minutes_bus,
        reply_time_in_minutes:calendar::int reply_time_in_minutes_cal,
        requester_wait_time_in_minutes:business::int requester_wait_time_in_minutes_bus,
        requester_wait_time_in_minutes:calendar::int requester_wait_time_in_minutes_cal,
        satisfaction_ratings:score::varchar satisfaction_rating_score,
        satisfaction_ratings:reason::varchar satisfaction_rating_reason
    FROM {{ source('zendesk_raw', 'tickets') }}
    LEFT JOIN {{ source('zendesk_raw', 'ticket_metrics') }} ON tickets.id = ticket_metrics.ticket_id
    LEFT JOIN {{ source('zendesk_raw', 'organizations') }} ON tickets.organization_id = organizations.id
    LEFT JOIN {{ source('orgm', 'account') }} ON organizations.id = account.zendesk__zendesk_organization_id__C
    LEFT JOIN {{ source('zendesk_raw', 'users') }} ON users.id = tickets.assignee_id
    LEFT JOIN {{ source('zendesk_raw', 'satisfaction_ratings') }} ON satisfaction_ratings.id = tickets.satisfaction_rating:id
)

select * from zendesk_ticket_details