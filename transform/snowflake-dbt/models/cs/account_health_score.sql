{{config({
    "materialized": "table",
    "schema": "cs",
    "post-hook": "{{ pg_import('staging.account_health_score', 'pg_update_account_health_score') }}"
  })
}}

WITH account_health_facts AS (
    SELECT
        account.sfid AS account_sfid,
        min(start_date__c)::date AS license_start_date,
        max(end_date__c)::date AS license_end_date,
        max(end_date__c) > current_date AS existing_customer,
        (least(max(end_date__c)::date, current_date) - min(start_date__c)::date) / 365 AS tenure_in_yrs,
        count(distinct tickets.ID) AS count_tickets_prev_90,
        current_date - max(tasks_filtered.createddate)::date AS days_since_last_task,
        min(CASE WHEN risk_opportuny.renewal_risk_status__c = 'At Risk' THEN 20 WHEN risk_opportuny.renewal_risk_status__c = 'Early Warning' THEN 50 ELSE NULL END) AS risk_override_score
    FROM {{ source('orgm', 'account') }}
        LEFT JOIN {{ source('orgm', 'opportunity') }}  ON opportunity.accountid = account.sfid AND iswon
        LEFT JOIN {{ source('orgm', 'opportunitylineitem') }} ON opportunitylineitem.opportunityid = opportunity.sfid
        LEFT JOIN {{ source('orgm', 'tasks_filtered') }} ON account.sfid = tasks_filtered.accountid
        LEFT JOIN {{ source('zendesk_raw', 'organizations') }} ON organizations.id = account.zendesk__zendesk_organization_id__c
        LEFT JOIN {{ source('zendesk_raw', 'tickets') }} ON tickets.organization_id = organizations.id AND tickets.created_at > current_date - INTERVAL '90 days'
        LEFT JOIN {{ source('orgm', 'opportunity') }} AS risk_opportuny ON risk_opportuny.accountid = account.sfid AND NOT risk_opportuny.isclosed AND risk_opportuny.renewal_risk_status__c IN ('At Risk','Early Warning')
    GROUP BY 1
), account_health_score AS (
    SELECT 
        account_health_facts.account_sfid,
        account_health_facts.tenure_in_yrs,
        CASE
            WHEN account_health_facts.tenure_in_yrs <= 0.5 THEN (1 - .75)
            WHEN account_health_facts.tenure_in_yrs <= 1 THEN (1 - .50)
            WHEN account_health_facts.tenure_in_yrs <= 2 THEN (1 - .25)
            WHEN account_health_facts.tenure_in_yrs > 2 THEN (1 - .10)
       END
       * 25 AS tenure_health_score,
       account_health_facts.license_end_date,
       CASE
            WHEN account_health_facts.license_end_date - current_date <= 15 THEN (1 - .90)
            WHEN account_health_facts.license_end_date - current_date <= 30 THEN (1 - .75)
            WHEN account_health_facts.license_end_date - current_date <= 60 THEN (1 - .25)
            WHEN account_health_facts.license_end_date - current_date <= 90 THEN (1 - .10)
            WHEN account_health_facts.license_end_date - current_date > 90 THEN (1 - .00)
       END
       * 25 AS license_end_date_health_score,
       account_health_facts.count_tickets_prev_90,
       CASE
            WHEN account_health_facts.count_tickets_prev_90 >= 5 THEN (1 - .75)
            WHEN account_health_facts.count_tickets_prev_90 >= 3 THEN (1 - .25)
            WHEN account_health_facts.count_tickets_prev_90 >= 1 THEN (1 - .00)
            WHEN account_health_facts.count_tickets_prev_90 = 0 THEN (1 - .50)
       END
       * 25 AS ticket_health_score,
       account_health_facts.days_since_last_task,
       CASE
            WHEN account_health_facts.days_since_last_task >= 90 OR account_health_facts.days_since_last_task IS NULL THEN (1 - .75)
            WHEN account_health_facts.days_since_last_task >= 60 THEN (1 - .50)
            WHEN account_health_facts.days_since_last_task >= 30 THEN (1 - .25)
            WHEN account_health_facts.days_since_last_task < 30 THEN (1 - .00)
       END
       * 25 AS task_health_score,
       risk_override_score AS risk_override_score,
       round(tenure_health_score + license_end_date_health_score + ticket_health_score + task_health_score,0) AS health_score_no_override,
       round(least(tenure_health_score + license_end_date_health_score + ticket_health_score + task_health_score,risk_override_score),0) AS health_score_w_override
    FROM account_health_facts
    WHERE account_health_facts.license_start_date <= current_date AND account_health_facts.license_end_date >= current_date
)

SELECT * FROM account_health_score