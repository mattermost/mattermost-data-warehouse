{% set rudder_relations = dbt_utils.get_rudder_relations(schema=["PORTAL_PROD"], database="RAW", table_inclusions="'PAGEVIEW_CLOUD_LANDING_PAGE','PAGEVIEW_CREATE_SIGNUP_PASSWORD','PAGEVIEW_VERIFY_EMAIL','PAGEVIEW_CREATE_WORKSPACE'") %}

{{ dbt_utils.union_relations(
    relations=rudder_relations,
    include=["user_id"]
) }}