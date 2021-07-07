{{config({
    "materialized": "incremental",
    "schema": "events",
    "tags":"union",
    "snowflake_warehouse": "transform_l"
  })
}}
-- depends_on: {{ ref('portal_events') }}
-- depends_on:  {{ ref('mobile_events') }}
-- depends_on: {{ ref('rudder_webapp_events') }} 
-- depends_on: {{ ref('segment_webapp_events') }} 
-- depends_on:{{ ref('segment_mobile_events') }}
-- depends_on: {{ ref('cloud_pageview_events') }} 
-- depends_on:{{ ref('cloud_portal_pageview_events') }}

{% set rudder_relations = get_rudder_relations(schema=["events"], database='ANALYTICS', 
                          table_inclusions="'portal_events','mobile_events','rudder_webapp_events','segment_webapp_events',
                                            'segment_mobile_events', 'cloud_pageview_events', 'cloud_portal_pageview_events'") %}
{{ union_relations(relations = rudder_relations) }}