from datetime import datetime

import pytest

from airflow import DAG


def test_should_create_pod_operators():
    from mattermost_dags.monitoring.events import get_pod_operators

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
        '--format-json > /airflow/xcom/return.json || true'
    ]

    assert result[1].task_id == 'check-new-tables-schema2'
    assert result[1].arguments == [
        'rudder list ${SNOWFLAKE_LOAD_DATABASE} schema2 -w ${SNOWFLAKE_LOAD_WAREHOUSE} '
        '-r ${SNOWFLAKE_LOAD_ROLE} --max-age {{ var.value.rudder_max_age }} '
        '--format-json > /airflow/xcom/return.json || true'
    ]


@pytest.mark.parametrize(
    "input,output",
    [
        [[], ''],
        [['a', 'b', 'c', 'd', 'e'], ' - a\n - b\n - c\n - d\n - e'],
    ],
)
def test_table_formatter(mocker, input, output):
    from mattermost_dags.monitoring.events import table_formatter

    # GIVEN: a list of items in xcom
    mock_ti = mocker.MagicMock()
    mock_ti.xcom_pull.return_value = {'new_tables': input}

    # WHEN: request to format items
    result = table_formatter('task-id')(ti=mock_ti)

    # THEN: expect to be in proper table
    assert result == output


@pytest.mark.parametrize(
    "input,output",
    [
        [{"new_tables": ['a', 'b']}, True],
        [{"new_tables": []}, False],
        [{"new_tables": None}, False],
        [{}, False],
        [None, False],
    ],
)
def test_short_circuit(mocker, input, output):
    from mattermost_dags.monitoring.events import short_circuit_on_no_new_tables

    # GIVEN: an xcom response
    mock_ti = mocker.MagicMock()
    mock_ti.xcom_pull.return_value = input

    # WHEN: request to short circuit
    result = short_circuit_on_no_new_tables('task-id')(ti=mock_ti)

    # THEN: expect to short circuit on empty input
    assert result == output
