import json
from pathlib import Path

import pytest
from responses import Response

from utils.run_dbt_cloud_job import trigger_dbt_run, poll_dbt_run


def test_should_trigger_run(responses):
    # GIVEN: DBT cloud ready to accept job
    rsp = Response(method="POST", url="https://cloud.getdbt.com/api/v2/accounts/1001/jobs/101/run/",
                   status=200, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                   body=with_response_body("job.200.json"))
    responses.add(rsp)

    # WHEN: trigger job
    run_id = trigger_dbt_run(101, "Test Job")

    # THEN: expect request to have been sent
    responses.assert_call_count("https://cloud.getdbt.com/api/v2/accounts/1001/jobs/101/run/", 1)
    assert json.loads(responses.calls[0].request.body) == {"cause": "Airflow run - Test Job"}

    # THEN: expect run id to have been properly loaded from response
    assert run_id == 10000


@pytest.mark.parametrize("status,content", [(400, 'job.400.json'), (404, 'job.404.json')])
def test_should_handle_failure(responses, status, content):
    # GIVEN: DBT cloud returns a non-200 response
    rsp = Response(method="POST", url="https://cloud.getdbt.com/api/v2/accounts/1001/jobs/101/run/",
                   status=status, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                   body=with_response_body(content))
    responses.add(rsp)

    # WHEN: trigger job
    run_id = trigger_dbt_run(101, "Test Job")

    # THEN: expect request to have been sent
    responses.assert_call_count("https://cloud.getdbt.com/api/v2/accounts/1001/jobs/101/run/", 1)
    assert json.loads(responses.calls[0].request.body) == {"cause": "Airflow run - Test Job"}

    # THEN: expect run id to be None
    assert run_id is None


def test_poll_dbt_run_job_completed(responses):
    # GIVEN: DBT cloud reports run as successful
    rsp = Response(method="GET", url="https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/",
                   status=200, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                   body=with_response_body("run.200.completed.json"))
    responses.add(rsp)

    # WHEN: poll for job completion
    poll_dbt_run(42)

    # THEN: expect run status to have been called once
    responses.assert_call_count("https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/", 1)


def test_poll_dbt_run_job_should_wait_for_completion(responses):
    # GIVEN: DBT cloud reports run as successful
    rsp = Response(method="GET", url="https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/",
                   status=200, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                   body=with_response_body("run.200.running.json"))
    responses.add(rsp)
    rsp = Response(method="GET", url="https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/",
                   status=200, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                   body=with_response_body("run.200.completed.json"))
    responses.add(rsp)

    # WHEN: poll for job completion
    poll_dbt_run(42)

    # THEN: expect run status to have been called twice
    responses.assert_call_count("https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/", 2)


def test_poll_dbt_run_job_failed_job(responses):
    # GIVEN: DBT cloud reports run as failed
    rsp = Response(method="GET", url="https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/",
                   status=200, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                   body=with_response_body("run.200.failed.json"))
    responses.add(rsp)

    # WHEN: poll for job completion, an exception must be raised
    with pytest.raises(Exception, match='Error running dbt cloud job -- make this link to dbt cloud'):
        poll_dbt_run(42)

    # THEN: expect run status to have been called once
    responses.assert_call_count("https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/", 1)


def test_poll_dbt_run_poll_timeout(responses):
    # GIVEN: DBT cloud reports run as failed
    rsp = Response(method="GET", url="https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/",
                   status=200, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                   body=with_response_body("run.200.running.json"))
    responses.add(rsp)

    # WHEN: poll for job completion, an exception must be raised
    with pytest.raises(Exception, match='dbt Cloud job ran out of time'):
        poll_dbt_run(42)

    # THEN: expect run status to have been called once
    responses.assert_call_count("https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/", 3)


@pytest.mark.parametrize("status,content", [(400, 'run.400.json'), (404, 'run.404.json')])
def test_should_handle_failure(responses, status, content):
    # GIVEN: DBT cloud returns a non-200 response
    rsp = Response(method="GET", url="https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/",
                   status=status, headers={"Authorization": f"Token test-dbt-key", "Content-Type": "application/json"},
                   body=with_response_body(content))
    responses.add(rsp)

    # WHEN: poll for job completion, retry on failures but fail due to timeout
    # TODO: reconsider how to handle failures from API
    with pytest.raises(Exception, match='dbt Cloud job ran out of time'):
        poll_dbt_run(42)

    # THEN: expect request to have been sent
    responses.assert_call_count("https://cloud.getdbt.com/api/v2/accounts/1001/runs/42/", 3)


def with_response_body(filename):
    """
    Loads a sample response body from a file.
    """
    with open(Path(__file__).parent / 'fixtures' / 'dbt' / filename) as fp:
        return fp.read()
