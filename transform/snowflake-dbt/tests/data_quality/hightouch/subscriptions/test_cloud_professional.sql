{{ config(
    tags = ['data-quality']
) }}

SELECT
    *
FROM
    {{ ref('dq_cloud_professional') }}
WHERE
    -- Fetch all opportunities/contacts/accounts not synced into salesforce.
    (
        (
            account_sfid IS NULL
            AND contact_sfid IS NULL
        )
        OR (
            opportunity_sfid IS NULL
        )
    )
    AND start_date IS NOT NULL
    AND start_date < DATEADD(DAY, -1, CURRENT_DATE())
