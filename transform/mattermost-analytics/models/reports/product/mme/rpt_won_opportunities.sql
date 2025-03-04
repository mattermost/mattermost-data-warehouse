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
        o.opportunity_id,
        o.amount,
        o.is_won,
        o.type,
        o.license_key__c as license_id,
        o.license_start_date__c as license_start_at,
        o.license_end_date__c as license_end_at,
        a.smb_mme__c as account_type,
        p.smb_mme__c as root_account_type,
        a.arr_current__c as account_arr,
        p.arr_current__c as root_account_arr,
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
    select
        license_telemetry_date as license_telemetry_date
        , license_id as license_id
        , server_id as server_id
        , customer_id as customer_id
        , license_name as license_name
    from {{ ref('stg_mm_telemetry_prod__license') }}
    where license_id is not null and installation_id is null
    qualify row_number() over (partition by server_id, license_id, license_telemetry_date order by timestamp desc) = 1
), mattermost2_license as (
    select
        license_telemetry_date as license_telemetry_date
        , license_id as license_id
        , server_id as server_id
        , customer_id as customer_id
        , license_name as license_name
    from {{ ref('stg_mattermost2__license') }}
    where license_id is not null
    qualify row_number() over (partition by server_id, license_id, license_telemetry_date order by timestamp desc) = 1
), all_telemetry_reported_licenses as (
    select license_id, server_id, license_telemetry_date from mm_telemetry_prod_license
    union
    select license_id, server_id, license_telemetry_date from mattermost2_license
), latest_active_users as (
    select
        server_id
        , last_activity_date
        , last_daily_active_users
        , last_monthly_active_users
        , last_count_registered_active_users
    from
        {{ ref('int_server_telemetry_summary') }}
    where server_id in (select server_id from all_telemetry_reported_licenses)
)
select
    o.opportunity_id
    , o.account_id
    , o.account_name
    , o.root_account_id
    , o.root_account_name
    , o.account_type
    , o.root_account_type
    , o.account_arr
    , o.root_account_arr
    , o.is_latest
    , o.license_id
    , kl.sku_short_name as license_sku
    , kl.licensed_seats
    , kl.expire_at
    , l.license_id is not null as has_telemetry
    , min(datediff('day', l.license_telemetry_date, current_date)) as days_since_last_license_telemetry
     , array_unique_agg(l.server_id) as servers
    -- Consider active servers with MAU > 0
    , array_unique_agg(st.server_id) as active_servers
    , array_size(active_servers) > 0 as has_user_activity
    , min(datediff('day', st.last_activity_date, current_date)) as days_since_last_user_activity
    , max(st.last_monthly_active_users) as max_last_monthly_active_users
from
    opportunities o
    left join all_telemetry_reported_licenses l on o.license_id = l.license_id
    left join {{ ref('int_known_licenses') }} kl on o.license_id = kl.license_id
    -- Keep only servers with telemetry and active users
    left join latest_active_users st on l.server_id = st.server_id and st.last_monthly_active_users > 0
group by all