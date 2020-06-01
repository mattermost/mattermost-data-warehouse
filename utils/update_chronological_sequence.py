import os
import pandas as pd
import sys
from extract.utils import snowflake_engine_factory, execute_query, execute_dataframe

def update_chronological_sequence():
    engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")

    query = f'''
    UPDATE ANALYTICS.EVENTS.USER_EVENTS_BY_DATE
    SET chronological_sequence = a.chronological_sequence,
        seconds_after_prev_event = a.seconds_after_prev_event
    FROM (
        SELECT id,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY min_timestamp) as chronological_sequence,
            datediff(second, lag(min_timestamp) over (partition by user_id order by min_timestamp), min_timestamp) as seconds_after_prev_event
        FROM ANALYTICS.EVENTS.USER_EVENTS_BY_DATE
    ) a
    WHERE user_events_by_date.max_timestamp::timestamp >= ((current_date || ' ' || current_time)::timestamp - interval '24 hours')::timestamp
    AND a.id = user_events_by_date.id;
    '''

    execute_query(engine, query)

if __name__ == "__main__":
    update_chronological_sequence()