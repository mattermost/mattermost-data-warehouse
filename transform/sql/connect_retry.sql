update orgm._trigger_log
set state = 'NEW', updated_at = now()
where state = 'FAILED'
and sf_message like 'unable to obtain exclusive access%';