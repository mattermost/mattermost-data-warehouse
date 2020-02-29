{{config({
    "materialized": 'table',
    "schema": "zendesk_raw"
  })
}}

WITH array_to_string (
    SELECT *
    FROM (
        SELECT 
            ticket.id AS ticket_id, 
            ARRAY_TO_STRING(custom_fields, ',') AS string
        FROM {{ source('zendesk_raw', 'tickets') }}
        ) AS splittable, 
        LATERAL SPLIT_TO_TABLE(splittable.string, '},{')
), custom_ticket_fields as (
    SELECT 
        ticket_id,
        split_part(split_part(value,':',2),',',1) as ticket_field_id, 
        CASE 
            WHEN REPLACE(split_part(split_part(value,':',3),'}',1),'"','') NOT IN ('','null') 
                THEN REPLACE(split_part(split_part(value,':',3),'}',1),'"','')
            ELSE NULL
        END AS field_value
    FROM array_to_string
    LEFT JOIN {{ source('zendesk_raw', 'ticket_fields') }} ON split_part(split_part(value,':',2),',',1).id = ticket_fields.id
)

SELECT * FROM custom_ticket_fields