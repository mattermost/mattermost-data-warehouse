from datetime import datetime

import pytest
from mattermost_dags.general._helpers import (
    HightouchApiException,
    hightouch_check_syncs,
    resolve_hightouch,
    stitch_check_extractions,
    stitch_check_loads,
    time_filter,
)


def test_stitch_check_extractions_pass(load_data):
    response = load_data('monitoring/extractions_success.json')
    failed_extrations = stitch_check_extractions(response)
    # Expected to return empty dict as no extractions failed
    assert failed_extrations == {}


def test_stitch_check_extractions_fail(load_data):
    response = load_data('monitoring/extractions_fail.json')
    failed_extractions = stitch_check_extractions(response)
    # Expected to return dict containing failed extractions, returning empty dict due to date filter
    assert failed_extractions == {}


def test_stitch_check_extractions_error():
    # Expected to raise exception since response body is empty
    with pytest.raises(TypeError):
        stitch_check_extractions({})


def test_stitch_check_loads_pass(load_data):
    response = load_data('monitoring/loads_success.json')
    failed_loads = stitch_check_loads(response)
    # Expected to return empty dict as no loads failed
    assert failed_loads == {}


def test_stitch_check_loads_fail(load_data):
    response = load_data('monitoring/loads_fail.json')
    failed_loads = stitch_check_loads(response)
    # Expected to return dict containing failed loads, returning empty dict due to date filter
    assert failed_loads == {}


def test_stitch_check_loads_error(load_data):
    # Expected to raise exception since response body is empty
    with pytest.raises(TypeError):
        stitch_check_loads({})


def test_hightouch_check_syncs_pass(load_data):
    response = load_data('monitoring/syncs_pass.json')
    failed_syncs = hightouch_check_syncs(response)
    # Expected to return empty dict as no loads failed
    assert failed_syncs == []


def test_hightouch_check_syncs_fail(load_data):
    response = load_data('monitoring/syncs_fail.json')
    failed_syncs = hightouch_check_syncs(response)
    # Expected to return dict containing failed loads
    assert failed_syncs == [
        {"id": 99999, "slug": "test_sync_1", "status": "warning", "updatedAt": "2022-12-07T12:59:20Z"}
    ]


def test_hightouch_check_syncs_error(load_data):
    # Expected to raise exception since response body is empty
    with pytest.raises(HightouchApiException):
        hightouch_check_syncs("{}")


def test_resolve_hightouch_success(task_instance, mocker):
    mattermost_operator_mock = mocker.patch('mattermost_dags.general._helpers.MattermostOperator')
    resolve_hightouch(ti=task_instance(True))
    # Does not call mattermost operator since no syncs failed
    mattermost_operator_mock.assert_not_called()


def test_resolve_hightouch_fail(task_instance, mocker):
    mattermost_operator_mock = mocker.patch('mattermost_dags.general._helpers.MattermostOperator')
    resolve_hightouch(ti=task_instance(False))
    # Calls the mattermost operator once
    mattermost_operator_mock.assert_called_once()


def test_resolve_hightouch_error(task_instance):
    # Expected to raise exception since Xcom does not contain dict
    with pytest.raises(ValueError):
        resolve_hightouch(ti=task_instance(None))


@pytest.mark.parametrize(
    "target_time, format, delta_hours, output",
    [
        (str(datetime.utcnow()), "%Y-%m-%d %H:%M:%S.%f", 1, True),
        (str(datetime.utcnow()), "%Y-%m-%d %H:%M:%S.%f", 2, True),
        (str(datetime.utcnow()), "%Y-%m-%d %H:%M:%S.%f", 0, False),
    ],
)
def test_time_filter(target_time, format, delta_hours, output):
    assert time_filter(target_time, format, delta_hours) == output


def test_time_filter_error():
    # Expected to raise exception since time format is incorrect
    with pytest.raises(ValueError):
        time_filter(str(datetime.utcnow()), "%Y-%m-%d %H:%M:%S", 2)
