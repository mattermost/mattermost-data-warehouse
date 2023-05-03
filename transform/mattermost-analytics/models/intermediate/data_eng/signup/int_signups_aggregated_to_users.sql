{{
    config({
        "materialized": "table",
        "incremental_strategy": "merge",
        "unique_key": ['portal_customer_id'],
        "merge_update_columns": ['verify_email']
    })
}}

WITH identifies as (
    SELECT user_id, 
        coalesce(portal_customer_id, context_traits_portal_customer_id) as portal_customer_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY RECEIVED_AT) AS row_number
    FROM
        {{ ref('stg_portal_prod__identifies') }}
    WHERE 
        coalesce(portal_customer_id, context_traits_portal_customer_id) IS NOT NULL and received_at >= '2023-04-04'
), pageviews as (
    SELECT
        user_id,
        event_table,
        received_at
    FROM
        {{ ref('stg_portal_prod__pageviews') }} 
    WHERE
        received_at >= '2023-04-04'
), signups as(
    SELECT
        identifies.portal_customer_id,
        -- Account is created when portal_customer_id exists
        true AS account_created, 
        -- Email is verified and user is redirected to `pageview_create_workspace` screen.
        MAX(CASE WHEN pageviews.event_table = 'pageview_create_workspace' THEN true ELSE false END) AS email_verified,
        -- Setting to false as we consider `workspace_installation_id` from stripe as source of truth 
        false AS workspace_created
    FROM
        pageviews
        JOIN (select * from identifies where row_number = 1) identifies
        ON pageviews.user_id = identifies.user_id
    GROUP BY
        identifies.portal_customer_id
)

select * from 
    signups
