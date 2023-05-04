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
), pageview_create_workspace as (
    SELECT
        user_id,
        pageview_id,
        timestamp
    FROM
        {{ ref('stg_portal_prod__pageview_create_workspace') }}
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
    timestamp
FROM
    rudder_portal_user_mappings
LEFT JOIN 
    pageview_create_workspace
ON pageview_email_verified.user_id = rudder_portal_user_mappings.user_id
)

select 
    portal_customer_id,
    account_created,
    email_verified
from user_signup_stages
qualify row_number() over (partition by portal_customer_id order by timestamp) = 1