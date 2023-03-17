{{ 
    dbt_profiler.get_profile(
        relation=ref("eg_performance_events"),
        exclude_measures=["std_dev_population", "std_dev_sample"],
        exclude_columns=["uuid_ts", 'id', 'anonymous_id', 'received_at', 'sent_at', 'original_timestamp', 'timestamp', 'context_ip', 'event']
    ) 
}}