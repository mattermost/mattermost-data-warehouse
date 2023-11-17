{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

with license_telemetry_range as (
    select server_id as server_id
        , license_id as license_id
        , min(license_date) as min_license_date
        , max(license_date) as max_license_date
    from {{ ref('stg_mm_telemetry_prod__license')}} 
    where license_id is not null and installation_id is null
    group by server_id, license_id
    union
    select server_id as server_id
        , license_id as license_id
        , min(license_date) as min_license_date
        , max(license_date) as max_license_date
    from {{ ref('stg_mattermost2__license')}} 
    where license_id is not null
    group by server_id, license_id
), spined as (
    select ltr.server_id as server_id
        , ltr.license_id as license_id
        , all_days.date_day::date as activity_date
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'license_id', 'activity_date']) }} AS daily_server_license_id
        , {{ dbt_utils.generate_surrogate_key(['server_id', 'activity_date']) }} AS daily_server_id
        , datediff(day, ltr.min_license_date, all_days.date_day::date) as age_in_days
    from
        license_telemetry_range ltr
        left join {{ ref('telemetry_days') }} all_days
            on all_days.date_day >= ltr.min_license_date and all_days.date_day <= ltr.max_license_date
) select s.daily_server_license_id
        , s.daily_server_id
        , s.server_id
        , s.license_id
        , s.activity_date
        , coalesce(rd.customer_id, sg.customer_id) as customer_id
        , coalesce(rd.license_name, sg.license_name) as license_name
    from spined s
    -- Rudderstack Telemetry
    left join {{ ref('stg_mm_telemetry_prod__license')}} rd 
        on s.server_id = rd.server_id and s.license_id = rd.license_id
    -- Segment Telemetry
    left join {{ ref('stg_mattermost2__license')}} sg 
        on s.server_id = sg.server_id and s.license_id = sg.license_id
    where server_id is not null and license_id is not null