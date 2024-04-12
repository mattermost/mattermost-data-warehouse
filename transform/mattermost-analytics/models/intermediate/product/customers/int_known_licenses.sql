-- List of all self-hosted license data from CWS and legacy licenses.
-- Performs deduplication in case a license exists both in CWS and legacy licenses.
with deduped_legacy_licenses as (
    select
        {{ dbt_utils.star(ref('stg_licenses__licenses')) }}
    from
        {{ ref('stg_licenses__licenses') }}
    group by all
)
select
    coalesce(cws.license_id, legacy.license_id) as license_id
    , coalesce(cws.company_name, legacy.company_name) as company_name
    , coalesce(cws.customer_email, legacy.contact_email) as contact_email
    , coalesce(cws.sku_short_name, 'Unkonown') as sku_short_name
    , coalesce(cws.expire_at, legacy.expire_at) as expire_at
    , coalesce(cws.is_trial, false) as is_trial
    , case
        when cws.license_id is not null and legacy.license_id is null then 'CWS'
        when cws.license_id is null and legacy.license_id is not null then 'Legacy'
        when cws.license_id is not null and legacy.license_id is not null then 'CWS and Legacy'
    end as source
from
    {{ ref('stg_cws__license') }} cws
    full outer join deduped_legacy_licenses legacy on cws.license_id = legacy.license_id
