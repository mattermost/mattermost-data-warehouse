-- List of all self-hosted license data from CWS, Salesforce, telemetry and legacy licenses.
-- Performs deduplication in case a license exists in multiple sources.
-- In case both CWS and legacy data are found, CWS data are preferred.
with deduped_legacy_licenses as (

    select
        {{ dbt_utils.star(ref('stg_licenses__licenses')) }}
    from
        {{ ref('stg_licenses__licenses') }}
    group by all

), telemetry_licenses as (

   select
        license_id
        , min(license_name) as license_name
        , min(licensed_seats) as licensed_seats
        , min(issued_at) as issued_at
        , min(starts_at) as starts_at
        , min(expire_at) as expire_at
    from
        {{ ref('int_server_license_daily') }}
    group by license_id
    -- Filter out licenses with ambiguous data
    having
        count(distinct licensed_seats) = 1
        and count(distinct starts_at) = 1
        and count(distinct expire_at) = 1
        and count(distinct customer_id) = 1

) , salesforce_licenses as (

    select
        o.opportunity_id,
        a.account_id,
        a.name as account_name,
        license_key__c as license_id,
        license_start_date__c as starts_at,
        license_end_date__c as expire_at,
        seats_from_name,
        a.arr_current__c as account_arr
    from
        {{ ref('stg_salesforce__opportunity') }} o
        left join {{ ref('stg_salesforce__account') }} a  on o.account_id = a.account_id
    where
        license_key__c is not null
        and o.stage_name = '6. Closed Won'
        -- License reported in multiple accounts or license has invalid value - temporarily remove
        and o.opportunity_id not in (
            '0063p00000yaF7lAAE',
            '0063p00000xRa3LAAS',
            '006S6000003skaPIAQ',
            '0063p00000zwAvlAAE',
            '006S6000003tQoKIAU',
            '006S6000003tXzJIAU',
            '0063600000g45XMAAY',
            '0063600000i6ZFsAAM',
            '006S6000003scWPIAY',
            '0063p00000yDKMNAA4'
        )
        and len(license_key__c) = 26
    -- In case of a license appearing multiple times, keep the latest opportunity
    qualify row_number() over (partition by license_key__c order by o.created_at desc) = 1

)

select
    coalesce(cws.license_id, legacy.license_id, sf.license_id, t.license_id) as license_id
    , coalesce(cws.company_name, legacy.company_name, sf.account_name) as company_name
    , coalesce(cws.customer_email, legacy.contact_email) as contact_email
    , coalesce(
        cws.sku_short_name,
        iff(t.license_name in ('E10', 'E20',  'starter', 'professional', 'enterprise'), t.license_name, null),
        'Unknown'
    ) as sku_short_name
    , t.license_name
    , coalesce(cws.starts_at, legacy.issued_at, sf.starts_at, t.starts_at) as starts_at
    , coalesce(cws.expire_at, legacy.expire_at, sf.expire_at, t.expire_at) as expire_at
    , cws.is_trial as is_trial
    , coalesce(cws.licensed_seats, sf.seats_from_name, t.licensed_seats) as licensed_seats
    -- Raw data from different sources in order to allow for comparison and detection of inconsistencies
    , cws.licensed_seats as cws_licensed_seats
    , sf.seats_from_name as salesforce_licensed_seats
    , t.licensed_seats as telemetry_licensed_seats
    , cws.starts_at as cws_starts_at
    , legacy.issued_at as legacy_starts_at
    , sf.starts_at as salesforce_starts_at
    , t.starts_at as telemetry_starts_at
    , cws.expire_at as cws_expire_at
    , legacy.expire_at as legacy_expire_at
    , sf.expire_at as salesforce_expire_at
    , t.expire_at as telemetry_expire_at
    , sf.account_arr as salesforce_account_arr

    -- Metadata related to source of information for each license.
    , cws.license_id is not null as in_cws
    , legacy.license_id is not null as in_legacy
    , sf.license_id is not null as in_salesforce
    , t.license_id is not null as in_telemetry
from
    {{ ref('stg_cws__license') }} cws
    full outer join deduped_legacy_licenses legacy using(license_id)
    full outer join salesforce_licenses sf using(license_id)
    full outer join telemetry_licenses t using(license_id)
