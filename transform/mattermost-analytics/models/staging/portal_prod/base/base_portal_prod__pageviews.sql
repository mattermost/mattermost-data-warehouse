{{ dbt_utils.union_relations(
    relations=[{{ source('portal_prod', 'PAGEVIEW_CLOUD_LANDING_PAGE') }}, {{ source('portal_prod', 'PAGEVIEW_CREATE_SIGNUP_PASSWORD') }}, {{ source('portal_prod', 'PAGEVIEW_VERIFY_EMAIL') }}, {{ source('portal_prod', 'PAGEVIEW_CREATE_WORKSPACE') }}],
    include=["user_id"]
) }}