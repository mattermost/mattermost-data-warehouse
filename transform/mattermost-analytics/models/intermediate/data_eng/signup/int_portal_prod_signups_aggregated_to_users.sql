{{
    config({
        "materialized": "table"
    })
}}

WITH identifies as (
    SELECT user_id, 
        coalesce(portal_customer_id, context_traits_portal_customer_id) as portal_customer_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY RECEIVED_AT) AS row_number
    FROM
        {{ ref('stg_portal_prod__identifies') }}
    WHERE 
        coalesce(portal_customer_id, context_traits_portal_customer_id) IS NOT NULL and received_at >= '2023-04-01'
), pageviews as (
    SELECT
        user_id,
        event_table,
        received_at
    FROM
        {{ ref('stg_portal_prod__pageviews') }} 
    WHERE
        received_at >= '2023-04-01'
), signups as(
    SELECT
        identifies.portal_customer_id,
        MAX(CASE WHEN pageviews.event_table = 'pageview_verify_email' THEN true ELSE false END) AS verify_email,
        MAX(CASE WHEN pageviews.event_table = 'pageview_create_workspace' THEN true ELSE false END) AS create_workspace,
        MAX(CASE WHEN pageviews.event_table = 'pageview_cloud_landing_page' THEN true ELSE false END) AS cloud_landing_page,
        MAX(CASE WHEN pageviews.event_table = 'pageview_create_signup_password' THEN true ELSE false END) AS create_signup_password,
        MAX(CASE WHEN pageviews.event_table = 'pageview_getting_started' THEN true ELSE false END) AS pageview_getting_started,
        MAX(CASE WHEN pageviews.event_table = 'pageview_true_up_review_cws' THEN true ELSE false END) AS pageview_true_up_review_cws,
        MAX(received_at) as date_updated
    FROM
        pageviews
        JOIN (select * from identifies where row_number = 1) identifies
        ON pageviews.user_id = identifies.user_id
    GROUP BY
        identifies.portal_customer_id
)

select * from 
    signups
