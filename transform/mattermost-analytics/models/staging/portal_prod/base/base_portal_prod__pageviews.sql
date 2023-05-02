{{ dbt_utils.join(
    table_1={{ source('portal_prod', 'PAGEVIEW_CLOUD_LANDING_PAGE') }},
    table_2={{ source('portal_prod', 'PAGEVIEW_CREATE_SIGNUP_PASSWORD') }},
    table_2={{ source('portal_prod', 'PAGEVIEW_VERIFY_EMAIL') }},
    table_2={{ source('portal_prod', 'PAGEVIEW_CREATE_WORKSPACE') }},
    keys=['user_id']
)}}