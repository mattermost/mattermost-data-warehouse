{% if execute %}
  {% if flags.FULL_REFRESH %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model. Exclude it from the run via the argument \"--exclude lead_status_hist\".") }}
  {% endif %}
{% endif %}

{{config({
    "materialized": "incremental",
    "schema": "orgm",
  })
}}

WITH todays_lead_status_updates AS (
    SELECT lead.sfid AS lead_sfid, 'MCL' AS status, NULL AS micro_status, most_recent_mcl_date__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE most_recent_mcl_date__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'MEL' AS status, NULL AS micro_status, most_recent_mel_date__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE most_recent_mel_date__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'MQL' AS status, NULL AS micro_status, most_recent_mql_date__c::date AS date, ownerid as owner,
        CASE
            WHEN most_recent_action_detail__c = 'In-Product Trial Request' THEN 'Trial Request - In-Product'
            WHEN most_recent_action_detail__c = 'mattermost.com Trial Request' THEN 'Trial Request - Website'
            WHEN most_recent_action__c = 'Cloud Trial' AND most_recent_action_detail__c = 'Cloud Trial Request' THEN 'Trial Request - Cloud'
            WHEN most_recent_action__c LIKE 'Contact Request%' AND most_recent_action_detail__c IN ('General','Hipchat Migration','Contact Us (Sales)','Government Inquiry','Remote Work Offer') THEN 'Contact Request - General'
            WHEN most_recent_action__c = 'Contact Request' AND most_recent_action_detail__c = 'Pricing' THEN 'Contact Request - Pricing'
            WHEN most_recent_action__c = 'Contact Request' AND most_recent_action_detail__c = 'In-Portal Contact Us' THEN 'Contact Request - In-Portal'
            WHEN most_recent_action__c = 'Contact Request' AND most_recent_action_detail__c = 'In-Cloud Contact Us' THEN 'Contact Request - In-Cloud'
            WHEN most_recent_action__c = 'Contact Request' AND most_recent_action_detail__c = 'Nonprofit License' THEN 'Contact Request - Nonprofit License'
            WHEN most_recent_action__c = 'Demo Request' THEN 'Contact Request - Demo'
            WHEN most_recent_action_detail__c = 'Cloud Workspace Creation' THEN 'Cloud - Ent/MM Workspace Creation'
            WHEN most_recent_action__c = 'Cloud Enterprise Quote Request' THEN 'Cloud - Enterprise Quote Request'
            WHEN most_recent_action__c IN ('Cloud Beta Trial', 'Cloud Signup') THEN 'Cloud - Beta'
            WHEN most_recent_action_detail__c LIKE 'Admin Advisor%' THEN 'Admin Advisor'
            ELSE NULL END as additional_details
    FROM {{ ref('lead') }}
    WHERE most_recent_mql_date__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'SCL' AS status, NULL AS micro_status, most_recent_scl_date__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE most_recent_scl_date__c::date > current_date - interval '1 days'
        AND (outreach__c::date != most_recent_scl_date__c::date OR outreach__c IS NULL)
        AND (connected__c::date != most_recent_scl_date__c::date OR connected__c IS NULL)
        AND (never_connected__c::date != most_recent_scl_date__c::date OR never_connected__c IS NULL)
        AND (discovery_call_booked__c::date != most_recent_scl_date__c::date OR discovery_call_booked__c IS NULL)
        AND (discovery_call_completed__c::date != most_recent_scl_date__c::date OR discovery_call_completed__c IS NULL)
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'SCL' AS status, CASE WHEN lead_status_minor__c like 'Outreach%' THEN lead_status_minor__c ELSE 'Outreach (Unknown)' END AS micro_status, outreach__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE outreach__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'SCL' AS status, 'Connected' AS micro_status, connected__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE connected__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'SCL' AS status, 'Discovery Call Booked' AS micro_status, discovery_call_booked__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE discovery_call_booked__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'SCL' AS status, 'Discovery Call Booked' AS micro_status, discovery_call_completed__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE discovery_call_completed__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'Recycle' AS status, RECYCLE_REASON__C AS micro_status, most_recent_recycle_date__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE most_recent_recycle_date__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'Junk' AS status, JUNK_REASON__C AS micro_status, first_not_a_lead_date__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE first_not_a_lead_date__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'QSO' AS status, NULL AS micro_status, most_recent_qso_date__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE most_recent_qso_date__c::date > current_date - interval '1 days'
    UNION ALL
    SELECT lead.sfid AS lead_sfid, 'QSC' AS status, NULL AS micro_status, most_recent_qsc_date__c::date AS date, ownerid as owner, NULL as additional_details
    FROM {{ ref('lead') }}
    WHERE most_recent_qsc_date__c::date > current_date - interval '1 days'
), lead_status_hist AS (
    SELECT lead_sfid, status, micro_status, date, owner, additional_details
    FROM todays_lead_status_updates
    
    {% if is_incremental() %}
        WHERE NOT EXISTS (
                            SELECT 1 
                            FROM {{this}} 
                            WHERE lead_sfid = todays_lead_status_updates.lead_sfid
                                AND status = todays_lead_status_updates.status
                                AND coalesce(micro_status,'') = coalesce(todays_lead_status_updates.micro_status,'')
                                AND date = todays_lead_status_updates.date
                        )
    {% endif %}
)

SELECT * FROM lead_status_hist



