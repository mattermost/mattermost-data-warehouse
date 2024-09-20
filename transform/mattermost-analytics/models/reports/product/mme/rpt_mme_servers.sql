with account_hierarchy as (
   select
        sys_connect_by_path(account_id, ' -> ') as path,
        account_id,
        name as account_name,
        parent_id,
        CONNECT_BY_ROOT account_id AS root_account_id,
        CONNECT_BY_ROOT name AS root_account_name
    from
        {{ ref('stg_salesforce__account') }}
        start with parent_id is null
        connect by parent_id = prior account_id
), opportunities as (
    select
        a.account_id,
        a.name as account_name,
        ar.root_account_id,
        ar.root_account_name,
        o.opportunity_id ,
        o.amount,
        o.is_won,
        o.type,
        o.license_key__c as license_id,
        o.license_start_date__c as license_start_at,
        o.license_end_date__c as license_end_at,
        a.smb_mme__c as account_type,
        p.smb_mme__c as root_account_type,
        o.created_at,
        row_number() over(partition by a.account_id order by o.created_at desc) = 1 as is_latest
    from
        {{ ref('stg_salesforce__opportunity') }} o
        left join {{ ref('stg_salesforce__account') }} a on o.account_id = a.account_id
        left join account_hierarchy ar on ar.account_id = a.account_id
        left join {{ ref('stg_salesforce__account') }} p on ar.root_account_id = p.account_id
    where
        stage_name ='6. Closed Won'
        and license_end_at > current_date
        and license_id is not null
        and not o.is_deleted
), mm_telemetry_prod_license as (
    select license_telemetry_date as license_telemetry_date
        , license_id as license_id
        , server_id as server_id
        , customer_id as customer_id
        , license_name as license_name
    from {{ ref('stg_mm_telemetry_prod__license') }}
    where license_id is not null and installation_id is null
    qualify row_number() over (partition by server_id, license_id, license_telemetry_date order by timestamp desc) = 1
), mattermost2_license as (
    select license_telemetry_date as license_telemetry_date
        , license_id as license_id
        , server_id as server_id
        , customer_id as customer_id
        , license_name as license_name
    from {{ ref('stg_mattermost2__license') }}
    where license_id is not null
    qualify row_number() over (partition by server_id, license_id, license_telemetry_date order by timestamp desc) = 1
), all_telemetry_reported_licenses as (
    select license_id, server_id from mm_telemetry_prod_license
    union
    select license_id, server_id from mattermost2_license
)
select
    account_id,
    account_name,
    root_account_id,
    root_account_name,
    account_type,
    root_account_type,
    max(l.license_id is not null) as has_telemetry,
    array_unique_agg(l.license_id) as license,
    array_unique_agg(l.server_id) as servers
from
    opportunities o
    left join all_telemetry_reported_licenses l on o.license_id = l.license_id
group by all