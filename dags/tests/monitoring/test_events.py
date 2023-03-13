from datetime import datetime

import pytest
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
        '-r ${SNOWFLAKE_LOAD_ROLE} --max-age {{ var.value.rudder_max_age }} '
        '--format-json > /airflow/xcom/return.json'
    ]

    assert result[1].task_id == 'check-new-tables-schema2'
    assert result[1].arguments == [
        'rudder list ${SNOWFLAKE_LOAD_DATABASE} schema2 -w ${SNOWFLAKE_LOAD_WAREHOUSE} '
        '-r ${SNOWFLAKE_LOAD_ROLE} --max-age {{ var.value.rudder_max_age }} '
        '--format-json > /airflow/xcom/return.json'
    ]


@pytest.mark.parametrize(
    "input,size,output",
    [
        [['a', 'b', 'c', 'd', 'e'], 2, '| a   | b   |\n|-----|-----|\n| c   | d   |\n| e   |     |'],
        [['a', 'b', 'c', 'd', 'e'], 3, '| a   | b   | c   |\n|-----|-----|-----|\n| d   | e   |     |'],
    ],
)
def test_table_formatter(mocker, input, size, output):
    from dags.monitoring.events import table_formatter

    # GIVEN: a list of items in xcom
    mock_ti = mocker.MagicMock()
    mock_ti.xcom_pull.return_value = {'new_tables': input}

    # WHEN: request to format items
    result = table_formatter('task-id', size=size)(ti=mock_ti)

    # THEN: expect to be in proper table
    assert result == output
