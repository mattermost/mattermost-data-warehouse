{{config({
    "materialized": "incremental",
    "schema": "mattermost",
    "post-hook": "{{ post_audit_delete_hook(this) }}"
  })
}}

select
    current_timestamp AS processed_at,
    object_id__c AS deleted_sfid
from {{ source('orgm_old','delete_history__c') }}

{% if is_incremental() %}
  where object_id__c not in (select deleted_sfid from {{ this }})
{% endif %}
