{{config({
    "materialized": "incremental",
    "schema": "mattermost",
    "post-hook": "{{ post_audit_delete_hook(this) }}"
  })
}}

select
    current_timestamp AS processed_at,
    object_id__c AS deleted_sfid
from {{ source('orgm', 'delete_history__c') }}

{% if is_incremental() %}
where deleted_date_time__c > (select max(processed_at) from {{ this }})
{% endif %}
