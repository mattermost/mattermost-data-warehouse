{% set rudder_relations = dbt_utils.get_relations_by_prefix(schema="PORTAL_PROD", database="RAW", prefix="PAGEVIEW_")

{{ dbt_utils.union_relations(
    relations=rudder_relations,
    include=["user_id"]
) }}