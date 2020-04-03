{{config({
    "materialized": 'table',
    "schema": "cs"
  })
}}

WITH zendesk_ticket_details AS (
    SELECT
        tickets.id AS ticket_id,
        tickets.raw_subject,
        tickets.description,
        organizations.name AS organization_name,
        account.sfid AS account_sfid,
        users.name AS assignee_name,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = 24889383 THEN custom_ticket_fields.field_value ELSE NULL END) AS enterprise_edition_version,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = 24998963 THEN custom_ticket_fields.field_value ELSE NULL END) AS customer_type,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = 24998983 THEN custom_ticket_fields.field_value ELSE NULL END) AS e20_customer_level_tier,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = 25430823 THEN custom_ticket_fields.field_value ELSE NULL END) AS category,
        CASE WHEN max(CASE WHEN custom_ticket_fields.ticket_field_id = 360029689292 THEN custom_ticket_fields.field_value ELSE NULL END) = 'true' THEN TRUE ELSE FALSE END AS pending_do_not_close,
        CASE WHEN tickets.tags LIKE '%premsupport%' THEN true ELSE false END AS premium_support,
        array_to_string(tickets.tags, ', ') AS tags,
        tickets.created_at,
        ticket_metrics.solved_at,
        tickets.status,
        ticket_metrics.agent_wait_time_in_minutes:business::int agent_wait_time_in_minutes_bus,
        ticket_metrics.agent_wait_time_in_minutes:calendar::int agent_wait_time_in_minutes_cal,
        ticket_metrics.first_resolution_time_in_minutes:business::int first_resolution_time_in_minutes_bus,
        ticket_metrics.first_resolution_time_in_minutes:calendar::int first_resolution_time_in_minutes_cal,
        ticket_metrics.full_resolution_time_in_minutes:business::int full_resolution_time_in_minutes_bus,
        ticket_metrics.full_resolution_time_in_minutes:calendar::int full_resolution_time_in_minutes_cal,
        ticket_metrics.on_hold_time_in_minutes:business::int on_hold_time_in_minutes_bus,
        ticket_metrics.on_hold_time_in_minutes:calendar::int on_hold_time_in_minutes_cal,
        ticket_metrics.reply_time_in_minutes:business::int reply_time_in_minutes_bus,
        ticket_metrics.reply_time_in_minutes:calendar::int reply_time_in_minutes_cal,
        ticket_metrics.requester_wait_time_in_minutes:business::int requester_wait_time_in_minutes_bus,
        ticket_metrics.requester_wait_time_in_minutes:calendar::int requester_wait_time_in_minutes_cal,
        tickets.satisfaction_rating:score::varchar satisfaction_rating_score,
        tickets.satisfaction_rating:reason::varchar satisfaction_rating_reason
    FROM {{ source('zendesk_raw', 'tickets') }}
    LEFT JOIN {{ source('zendesk_raw', 'ticket_metrics') }} ON tickets.id = ticket_metrics.ticket_id
    LEFT JOIN {{ source('zendesk_raw', 'organizations') }} ON tickets.organization_id = organizations.id
    LEFT JOIN {{ source('orgm', 'account') }} ON organizations.id = account.zendesk__zendesk_organization_id__C
    LEFT JOIN {{ source('zendesk_raw', 'users') }} ON users.id = tickets.assignee_id
    LEFT JOIN {{ ref('custom_ticket_fields') }} ON tickets.id = custom_ticket_fields.ticket_id
    GROUP BY 1, 2, 3, 4, 5, 6, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
)

select * from zendesk_ticket_details