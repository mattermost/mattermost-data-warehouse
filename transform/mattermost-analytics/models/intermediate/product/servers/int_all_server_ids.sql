--
-- List of all known server ids
--

-- Server side telemetry
select
    distinct server_id
from
    {{ ref('int_server_telemetry_legacy_latest_daily') }}
union
select
    distinct server_id
from
    {{ ref('int_server_telemetry_latest_daily') }}
union
-- User telemetry
select
    distinct server_id
from
    {{ ref('int_user_active_days_legacy_telemetry') }}
union
select
    distinct server_id
from
    {{ ref('int_user_active_days_server_telemetry') }}
union
-- Diagnostics
select
    distinct server_id
from
    {{ ref('stg_diagnostics__log_entries') }}
