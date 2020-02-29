{{config({
    "materialized": 'table',
    "schema": "zendesk_raw"
  })
}}

WITH array_to_string AS (
    SELECT 
        *, 
        split_part(split_part(value,':',2),',',1) as ticket_field_id,
        REPLACE(split_part(split_part(value,':',3),'}',1),'"','') AS field_value
    FROM (
        SELECT 
            tickets.id AS ticket_id, 
            ARRAY_TO_STRING(custom_fields, ',') AS string
        FROM {{ source('zendesk_raw', 'tickets') }}
    ) AS splittable, LATERAL SPLIT_TO_TABLE(splittable.string, '},{')
), custom_ticket_fields as (
    SELECT 
        ticket_fields.title,
        ticket_id,
        ticket_field_id,
        CASE WHEN field_value NOT IN ('','null') THEN field_value ELSE NULL END AS field_value
    FROM array_to_string
    LEFT JOIN {{ source('zendesk_raw', 'ticket_fields') }} ON array_to_string.ticket_field_id = ticket_fields.id
)

SELECT * FROM custom_ticket_fields