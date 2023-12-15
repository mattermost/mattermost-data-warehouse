from mattermost.operators.mattermost_operator import MattermostOperator


def test_create_full_config(dag, full_config):
    op = MattermostOperator(task_id="mattermost_task", dag=dag, **full_config)

    assert op.mattermost_conn_id == "test-webhook-connection"
    assert op.text == "Test message"
    assert op.channel == "test-channel"
    assert op.username == "joe"
    assert op.type == "custom_test"
    assert op.props == {"card": "Test Card **Works**"}


def test_create_min_config(dag, min_config):
    op = MattermostOperator(task_id="mattermost_task", dag=dag, **min_config)

    assert op.mattermost_conn_id == "test-webhook-connection"
    assert op.text == "Test message"
    assert op.channel is None
    assert op.username is None
    assert op.type is None
    assert op.props is None


def test_template_fields(dag, full_config):
    operator = MattermostOperator(task_id="mattermost_task", dag=dag, **full_config)

    template_fields = ('text', 'channel', 'username', 'type', 'props', 'attachments')

    assert template_fields == operator.template_fields


def test_hook_is_created_properly(dag, full_config):
    operator = MattermostOperator(task_id="mattermost_task", dag=dag, **full_config)

    hook = operator.hook()
    assert hook.http_conn_id == "test-webhook-connection"
    assert hook.text == "Test message"
    assert hook.channel == "test-channel"
    assert hook.username == "joe"
    assert hook.type == "custom_test"
    assert hook.props == {"card": "Test Card **Works**"}
