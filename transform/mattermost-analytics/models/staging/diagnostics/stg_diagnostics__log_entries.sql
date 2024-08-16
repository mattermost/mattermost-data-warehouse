with source as (

    select * from {{ source('diagnostics', 'log_entries') }}

),

renamed as (
    select
        to_date(logdate) as log_date,
        to_timestamp(logdate|| ' ' || logtime) as log_at,
        edge,
        cip as server_ip,
        -- Temporarily enable
        cs_uri_query,
        mattermost_analytics.parse_qs(cs_uri_query) as _parsed_cs_uri_query,
        _parsed_cs_uri_query:id::varchar as server_id,
        _parsed_cs_uri_query:b::varchar as _security_build,
        try_to_decimal(split_part(_security_build, '.', 1)) as version_major,
        try_to_decimal(split_part(_security_build, '.', 2)) as version_minor,
        try_to_decimal(split_part(_security_build, '.', 3)) as version_patch,
        split_part(_security_build, '.', 1) || '.' || split_part(_security_build, '.', 2) || '.' || split_part(_security_build, '.', 3) as version_full,
        _parsed_cs_uri_query:be::varchar = 'true' as is_enterprise_ready,
        _parsed_cs_uri_query:db::varchar as database_type,
        _parsed_cs_uri_query:os::varchar as operating_system,
        _parsed_cs_uri_query:uc::int as count_users,
        _parsed_cs_uri_query:tc::int as count_teams,
        _parsed_cs_uri_query:auc::int as count_active_users,
        _parsed_cs_uri_query:ut::int = 1 as has_run_unit_tests,
        regexp_substr(
            _security_build,
            '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}$'
        ) is null
        and regexp_substr(
            _security_build, '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}$') is null
        -- Extra check to properly handle cloud builds
        and regexp_substr(
            _security_build,
            '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.cloud-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]+$'
        ) is null
        -- New build format
        and regexp_substr(
            _security_build,
            '^[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{1,2}\.{1}[0-9]{10,}$'
        ) is null
        as is_custom_build_version_format

        -- Ignoring these columns as they either have the same value always or there's no value in the data
        -- bytessent,
        -- method,
        -- host,
        -- uri,
        -- status,
        -- creferrer,
        -- cookie,
        -- x_edge_result_type,
        -- x_edge_request_id,
        -- x_host_header,
        -- protocol,
        -- cs_bytes,
        -- time_taken,
        -- x_forwarded_for,
        -- ssl_protocol,
        -- ssl_cipher,
        -- x_edge_response_result_type,
        -- cs_protocol_version,
        -- file_status,
        -- file_encrypted_fields

    from source

    where
        -- Only security endpoint contains useful information. All other requests do not contain useful information
        URI = '/security'
        and method = 'GET'
        -- Keep only requests sent by Mattermost servers
        and useragent like 'Go-http-client/%'
        -- Keep only if log data exists

        -- Sanity check
        and to_date(logdate) <= CURRENT_DATE

        -- Remove invalid records
        and not (cs_uri_query like any ('%BUMP_RELEASE%', '%plyr-1%'))
)

select * from renamed
