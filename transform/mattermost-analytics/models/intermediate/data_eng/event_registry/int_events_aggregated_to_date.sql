{{
    config({
        "tags":"hourly",
    })
}}

{{
    dbt_utils.union_relations(
        relations=[
            ref('int_hacktoberboard_prod_aggregated_to_date'),
            ref('int_incident_response_prod_aggregated_to_date'),
            ref('int_mattermost_docs_aggregated_to_date'),
            ref('int_mattermostcom_aggregated_to_date'),
            ref('int_mm_mobile_prod_aggregated_to_date'),
            ref('int_mm_plugin_prod_aggregated_to_date'),
            ref('int_mm_telemetry_prod_aggregated_to_date'),
            ref('int_mm_telemetry_rc_aggregated_to_date'),
            ref('int_portal_prod_aggregated_to_date'),
        ],
        source_column_name=None
    )
}}

