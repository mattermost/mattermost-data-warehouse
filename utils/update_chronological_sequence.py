import os

from extract.utils import execute_query, snowflake_engine_factory


def update_chronological_sequence():
    engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")

    query = '''
    UPDATE ANALYTICS.EVENTS.USER_EVENTS_BY_DATE
    SET chronological_sequence = a.chronological_sequence,
        seconds_after_prev_event = a.seconds_after_prev_event
    FROM (
        SELECT id,
            updated_at,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY min_timestamp) as chronological_sequence,
            datediff(
                second,
                lag(min_timestamp) over (partition by user_id order by min_timestamp),
                min_timestamp
            ) as seconds_after_prev_event
        FROM ANALYTICS.EVENTS.USER_EVENTS_BY_DATE
        WHERE length(user_id) < 36
        AND user_id IS NOT NULL
    ) a
    WHERE
    user_events_by_date.updated_at::timestamp = (
        SELECT MAX(UPDATED_AT)::timestamp FROM analytics.events.user_events_by_date
    )
    AND a.id = user_events_by_date.id;
    '''

    execute_query(engine, query)


if __name__ == "__main__":
    update_chronological_sequence()
