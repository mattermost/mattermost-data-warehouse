import json

import pytest

from utils.run_dbt_cloud_job import poll_dbt_run, trigger_dbt_run

EXPECTED_RUN_TRIGGER_URL = "https://cloud.getdbt.com/api/v2/accounts/1001/jobs/101/run/"
EXPECTED_RUN_POLL_URL = "https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/"

# Customize defaults for given_request_to
__MOCK_REQUEST_DEFAULTS = {
    'dir': 'dbt',
    'headers': {"Authorization": "Token test-dbt-key", "Content-Type": "application/json"},
}


def test_should_trigger_run(responses, given_request_to):
    # GIVEN: DBT cloud ready to accept job
    given_request_to(EXPECTED_RUN_TRIGGER_URL, "job.200.json", method="POST")

    # WHEN: trigger job
    run_id = trigger_dbt_run(101, "Test Job")

    # THEN: expect request to have been sent
    responses.assert_call_count(EXPECTED_RUN_TRIGGER_URL, 1)
    assert json.loads(responses.calls[0].request.body) == {"cause": "Airflow run - Test Job"}

    # THEN: expect run id to have been properly loaded from response
    assert run_id == 10000


@pytest.mark.parametrize("status,content", [(400, 'job.400.json'), (404, 'job.404.json')])
def test_should_handle_failure(responses, given_request_to, status, content):
    # GIVEN: DBT cloud returns a non-200 response
    given_request_to(EXPECTED_RUN_TRIGGER_URL, content, method="POST", status=status)

    # WHEN: trigger job
    run_id = trigger_dbt_run(101, "Test Job")

    # THEN: expect request to have been sent
    responses.assert_call_count(EXPECTED_RUN_TRIGGER_URL, 1)
    assert json.loads(responses.calls[0].request.body) == {"cause": "Airflow run - Test Job"}

    # THEN: expect run id to be None
    assert run_id is None


def test_poll_dbt_run_job_completed(responses, given_request_to):
    # GIVEN: DBT cloud reports run as successful
    given_request_to(EXPECTED_RUN_POLL_URL, "run.200.completed.json")

    # WHEN: poll for job completion
    poll_dbt_run(42)

    # THEN: expect run status to have been called once
    responses.assert_call_count(EXPECTED_RUN_POLL_URL, 1)


def test_poll_dbt_run_job_should_wait_for_completion(responses, given_request_to):
    # GIVEN: DBT cloud reports run as running then successful
    given_request_to(EXPECTED_RUN_POLL_URL, "run.200.running.json")
    given_request_to(EXPECTED_RUN_POLL_URL, "run.200.completed.json")

    # WHEN: poll for job completion
    poll_dbt_run(42)

    # THEN: expect run status to have been called twice
    responses.assert_call_count(EXPECTED_RUN_POLL_URL, 2)


def test_poll_dbt_run_job_failed_job(responses, given_request_to):
    # GIVEN: DBT cloud reports run as failed
    given_request_to(EXPECTED_RUN_POLL_URL, "run.200.failed.json")

    # WHEN: poll for job completion, an exception must be raised
    with pytest.raises(Exception, match='Error running dbt cloud job -- make this link to dbt cloud'):
        poll_dbt_run(42)

    # THEN: expect run status to have been called once
    responses.assert_call_count(EXPECTED_RUN_POLL_URL, 1)


def test_poll_dbt_run_poll_timeout(responses, given_request_to):
    # GIVEN: DBT cloud reports run as running
    given_request_to(EXPECTED_RUN_POLL_URL, "run.200.running.json")

    # WHEN: poll for job completion, an exception must be raised
    with pytest.raises(Exception, match='dbt Cloud job ran out of time'):
        poll_dbt_run(42)

    # THEN: expect run status to have been called once
    responses.assert_call_count(EXPECTED_RUN_POLL_URL, 3)


@pytest.mark.parametrize("status,content", [(400, 'run.400.json'), (404, 'run.404.json')])
def test_should_handle_non_2xx_responses(responses, given_request_to, status, content):
    # GIVEN: DBT cloud returns a non-200 response
    given_request_to(EXPECTED_RUN_POLL_URL, content, status=status)

    # WHEN: poll for job completion, retry on failures but fail due to timeout
    # TODO: reconsider how to handle failures from API
    with pytest.raises(Exception, match='dbt Cloud job ran out of time'):
        poll_dbt_run(42)

    # THEN: expect request to have been sent
    responses.assert_call_count(EXPECTED_RUN_POLL_URL, 3)
