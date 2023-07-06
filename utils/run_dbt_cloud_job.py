import os
import sys
import time
from logging import getLogger

import requests

logger = getLogger(__name__)

BASE_URL = "https://cloud.getdbt.com/api/v2"

token = os.getenv("DBT_CLOUD_API_KEY")
account_id = os.getenv("DBT_CLOUD_API_ACCOUNT_ID")
timeout = int(os.getenv("DBT_JOB_TIMEOUT", 2700))
headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}


def trigger_dbt_run(job_id: int, job_description: str) -> int:
    data = {"cause": f"Airflow run - {job_description}"}
    url = f"{BASE_URL}/accounts/{account_id}/jobs/{job_id}/run/"
    logger.info(f"Calling dbt cloud API with URL {url}")
    resp = requests.request(
        "POST",
        url,
        json=data,
        headers=headers,
    )

    if resp.status_code == 200:
        run_id = resp.json()["data"]["id"]
        if not run_id:
            logger.info(f"No run id returned, response from CloudDBT was: {resp.content}")
            raise Exception("Unable to get run id")
        logger.info(f"Triggered dbt cloud job run with ID: {run_id}")
        return run_id
    else:
        logger.error(f"Failed to trigger dbt cloud job run, reason: {resp.content}")


def poll_dbt_run(run_id: int):
    current_time = int(time.time())
    timeout_time = current_time + timeout

    status = "running"

    while current_time <= timeout_time and status == "running":
        resp = requests.get(f"{BASE_URL}/accounts/{account_id}/runs/{run_id}/", headers=headers)

        logger.info(f"Checking dbt cloud run {run_id} status: {status}")

        if resp.status_code == 200:
            payload = resp.json()["data"]
            logger.info(payload)
            if payload["is_complete"]:
                if payload["is_error"]:
                    raise Exception("Error running dbt cloud job -- make this link to dbt cloud")
                status = "finished"

        current_time = int(time.time())
        time.sleep(20)

    if status == "running":
        raise Exception("dbt Cloud job ran out of time")


if __name__ == "__main__":
    try:
        run_id = trigger_dbt_run(sys.argv[1], sys.argv[2])
        poll_dbt_run(run_id)
    except Exception as e:
        logger.error("An exception occurred", e)
        raise
