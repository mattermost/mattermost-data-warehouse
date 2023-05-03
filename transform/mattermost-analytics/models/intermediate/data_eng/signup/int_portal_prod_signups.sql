WITH signups as(
SELECT
    identifies.user_id,
    identifies.portal_customer_id,
    MAX(CASE WHEN pageviews.event_type = 'PAGEVIEW_VERIFY_EMAIL' THEN true ELSE false END) AS account_created,
    MAX(CASE WHEN pageviews.event_type = 'PAGEVIEW_CREATE_WORKSPACE' THEN true ELSE false END) AS email_verified,
    null AS workspace_created
FROM
    {{ ref('stg_portal_prod__pageviews') }} pageviews
    JOIN {{ ref('stg_portal_prod__identifies') }} identifies
    ON pageviews.user_id = identifies.user_id
GROUP BY
    identifies.user_id,
    identifies.portal_customer_id
)

select * from 
    signups
