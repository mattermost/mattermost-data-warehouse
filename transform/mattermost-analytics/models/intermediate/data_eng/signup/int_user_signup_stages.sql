{{
    config({
        "materialized": "table"
    })
}}
WITH rudder_portal_user_mappings as (
    SELECT
        user_id,
        portal_customer_id
    FROM
        {{ ref('int_rudder_portal_user_mapping') }} 
), pageview_email_verified as (
    SELECT
        user_id,
        pageview_id,
        timestamp
    FROM
        {{ ref('stg_portal_prod__pageview_email_verified') }}
), user_signup_stages as ( 
SELECT
    portal_customer_id,
    -- Account is created when portal_customer_id exists
    true AS account_created, 
    -- Email is verified and user is redirected to `pageview_create_workspace` screen.
    CASE 
        WHEN pageview_email_verified.pageview_id IS NOT NULL 
            THEN TRUE 
        ELSE FALSE 
    END AS email_verified,
    qualify row_number() over (partition by portal_customer_id order by timestamp) as row_number
FROM
    rudder_portal_user_mappings
LEFT JOIN 
    pageview_email_verified
ON pageview_email_verified.user_id = rudder_portal_user_mappings.user_id
)

select 
    * from user_signup_stages
