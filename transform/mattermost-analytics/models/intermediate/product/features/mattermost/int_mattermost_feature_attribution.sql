{{
    config({
        "materialized": "view"
    })
}}

select
    {{ dbt_utils.star(from=ref('int_client_feature_attribution'), quote_identifiers=False) }}
from
    {{ ref('int_client_feature_attribution') }}

union all

select
    {{ dbt_utils.star(from=ref('int_server_feature_attribution'), quote_identifiers=False) }}
from
    {{ ref('int_server_feature_attribution') }}