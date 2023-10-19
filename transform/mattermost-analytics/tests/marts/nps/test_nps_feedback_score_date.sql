-- models/marts/nps/dim_nps_feedback.sql

--Test to check that score_date is equal to feedback_date when not null
with test_data as (
    select distinct server_id
        , user_id
        , score_date
        , feedback_date
    from {{ ref('dim_nps_feedback') }}
    where score is not null
) select
    case
        when score_date is not null and score_date != feedback_date then
            'FAIL' as status,
            'score_date should be equal to feedback_date when not null' as message
        else
            'PASS' as status,
            null as message
    end as status
from test_data;
