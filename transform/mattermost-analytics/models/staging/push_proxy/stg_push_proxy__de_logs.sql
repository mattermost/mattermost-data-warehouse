with source as (

    select * from {{ source('push_proxy', 'de_logs') }}

),

renamed as (

    select
        type
        , requesttime as request_at
        , elb
        , client_and_port
        , target_and_port
        , request_processing_time
        , target_processing_time
        , response_processing_time
        , elb_status_code
        , target_status_code
        , received_bytes
        , sent_bytes
        , request
        , user_agent
        , ssl_cipher
        , ssl_protocol
        , target_group_arn
        , trace_id
        , domain_name
        , chosen_cert_arn
        , matched_rule_priority
        , request_creation_time
        , actions_executed

    from source

)

select * from renamed
