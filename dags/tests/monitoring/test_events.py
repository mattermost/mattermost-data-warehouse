from datetime import datetime

from airflow import DAG


def test_should_create_pod_operators():
    from dags.monitoring.events import get_pod_operators

    # GIVEN: a dag
    dag = DAG(
        'test-dag',
        start_date=datetime(2021, 1, 1),
    )

    # WHEN: request to create pod operators
    result = get_pod_operators(dag)

    # THEN: a pod operators to have been created for each schema
    assert len(result) == 2
    assert result[0].task_id == 'check-new-tables-schema1'
    assert result[0].arguments == [
        'rudder list ${SNOWFLAKE_LOAD_DATABASE} schema1 -w ${SNOWFLAKE_LOAD_WAREHOUSE} '
        '-r ${SNOWFLAKE_LOAD_ROLE} --max-age {{ var.value.rudder_max_age }}'
    ]

    assert result[1].task_id == 'check-new-tables-schema2'
    assert result[1].arguments == [
        'rudder list ${SNOWFLAKE_LOAD_DATABASE} schema2 -w ${SNOWFLAKE_LOAD_WAREHOUSE} '
        '-r ${SNOWFLAKE_LOAD_ROLE} --max-age {{ var.value.rudder_max_age }}'
    ]
