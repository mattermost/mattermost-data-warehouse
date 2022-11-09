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
        assignee.name AS assignee_name,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = '24889383' THEN custom_ticket_fields.field_value ELSE NULL END) AS enterprise_edition_version,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = '24998963' THEN custom_ticket_fields.field_value ELSE NULL END) AS customer_type,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = '25430823' THEN custom_ticket_fields.field_value ELSE NULL END) AS category,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = '25430823' THEN custom_ticket_fields.field_value ELSE NULL END) AS tech_support_category,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = '360036694171' THEN custom_ticket_fields.field_value ELSE NULL END) AS business_impact,
        max(CASE WHEN custom_ticket_fields.ticket_field_id = '360031056451' THEN custom_ticket_fields.field_value ELSE NULL END) AS sales_billings_support_category,
        CASE WHEN max(CASE WHEN custom_ticket_fields.ticket_field_id = '360029689292' THEN custom_ticket_fields.field_value ELSE NULL END) = 'true' THEN TRUE ELSE FALSE END AS pending_do_not_close,
        CASE WHEN tickets.tags LIKE '%premsupport%' THEN true ELSE false END AS premium_support,
        array_to_string(tickets.tags, ', ') AS tags,
        tickets.created_at,
        ticket_metrics.solved_at,
        tickets.status,
        tickets.priority,
        ticket_forms.name as form_name,
        coalesce(organizations.organization_fields:at_risk_customer,false)::boolean as account_at_risk,
        coalesce(organizations.organization_fields:_oppts_at_risk_::int,0) as account_oppt_at_risk,
        coalesce(organizations.organization_fields:_oppts_early_warning_::int,0) as account_oppt_early_warning,
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
        tickets.satisfaction_rating:reason::varchar satisfaction_rating_reason,
        MAX(ticket_comments.created_at) AS last_comment_at,
        MAX(CASE WHEN commentor.role IN ('agent','admin') AND ticket_comments.public THEN ticket_comments.created_at ELSE NULL END) AS last_non_enduser_comment_at,
        MAX(CASE WHEN commentor.role = 'end-user' AND ticket_comments.public THEN ticket_comments.created_at ELSE NULL END) AS last_enduser_comment_at,
        requester.name AS requester_name,
        submitter.name AS submitter_name,
        submitter.email AS submitter_email
    FROM {{ source('zendesk_raw', 'tickets') }}
    LEFT JOIN {{ source('zendesk_raw', 'ticket_metrics') }} ON tickets.id = ticket_metrics.ticket_id
    LEFT JOIN {{ source('zendesk_raw', 'organizations') }} ON tickets.organization_id = organizations.id
    LEFT JOIN {{ ref( 'account') }} ON left(organizations.external_id,15) = left(account.sfid,15)
    LEFT JOIN {{ source('zendesk_raw', 'users') }} AS assignee ON assignee.id = tickets.assignee_id
    LEFT JOIN {{ ref('custom_ticket_fields') }} ON tickets.id = custom_ticket_fields.ticket_id
    LEFT JOIN {{ source('zendesk_raw', 'ticket_comments') }} ON tickets.id = ticket_comments.ticket_id
    LEFT JOIN {{ source('zendesk_raw', 'users') }} AS commentor ON commentor.id = ticket_comments.author_id
    LEFT JOIN {{ source('zendesk_raw', 'users') }} AS requester ON requester.id = tickets.requester_id
    LEFT JOIN {{ source('zendesk_raw', 'users') }} AS submitter ON submitter.id = tickets.submitter_id
    LEFT JOIN {{ source('zendesk_raw', 'ticket_forms') }} ON ticket_forms.id = tickets.ticket_form_id
    GROUP BY 1, 2, 3, 4, 5, 6, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 41, 42, 43
)

select * from zendesk_ticket_details