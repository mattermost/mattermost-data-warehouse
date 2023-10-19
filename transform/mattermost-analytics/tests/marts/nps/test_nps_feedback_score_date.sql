-- models/marts/nps/dim_nps_feedback.sql

-- Custom test to check that score_date is equal to feedback_date when not null

{{ config(
    severity = 'warn'
) }}

with test_data as (
    select
        score_date,
        feedback_date
    from {{ ref('dim_nps_feedback') }}
    where score is not null
) select
    case
        when score_date is not null  
        and score_date != feedback_date then 'FAIL' END 
        as status
from test_data 
where status = 'FAIL'
