import urllib


def test_create_alert_body(config_alert_context):
    from mattermost_dags.airflow_utils import create_alert_body

    task_params = urllib.parse.urlencode({"execution_date": config_alert_context['ts']})
    body = create_alert_body(config_alert_context)
    assert ':red_circle: Test Exception message' in body
    assert (
        '**Dag**: [test_utils_dag](https://airflow.dataeng.internal.mattermost.com/tree?dag_id=test_utils_dag)' in body
    )
    assert (
        f'**Task**: [test_task](https://airflow.dataeng.internal.mattermost.com/task?dag_id=test_utils_dag&task_id=test_task&{task_params})'  # noqa: E501
        in body
    )


def test_send_alert(config_alert_context, mocker):
    from mattermost_dags.airflow_utils import send_alert

    mattermost_operator = mocker.patch("mattermost_dags.airflow_utils.MattermostOperator")
    create_alert_body = mocker.patch("mattermost_dags.airflow_utils.create_alert_body")
    mattermost_operator.return_value = mocker.Mock()
    create_alert_body.return_value = 'test message'
    send_alert(config_alert_context)
    mattermost_operator.assert_called_once_with(
        mattermost_conn_id='mattermost', text='test message', username='Airflow', task_id='test_task_failed_alert'
    )
    create_alert_body.assert_called_once()
