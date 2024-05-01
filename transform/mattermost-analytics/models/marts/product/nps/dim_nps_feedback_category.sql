{{
config({
    "materialized": 'incremental',
    "incremental_strategy": "append",
    "unique_key": ['feedback_id'],
  })
}}

select nf.user_id
       , nf.feedback as feedback
       , nf.event_date as feedback_date
       , nf.timestamp as feedback_timestamp
       , md5(nf.feedback) as feedback_id
       , cast(null as varchar) as feedback_category
       , cast(null as varchar) as feedback_subcategory
    from {{ ref('int_nps_feedback') }} nf 
    where nf.feedback is not null 
    {% if is_incremental() %}
       -- this filter will only be applied on an incremental run
      and nf.timestamp >= (select max(feedback_timestamp) from {{ this }})
    {% endif %}