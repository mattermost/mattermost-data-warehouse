import os
import sys
import time

import requests

BASE_URL = "https://cloud.getdbt.com/api/v2"

token = os.getenv("DBT_CLOUD_API_KEY")
account_id = os.getenv("DBT_CLOUD_API_ACCOUNT_ID")
timeout = int(os.getenv("DBT_JOB_TIMEOUT", 2700))
headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}


def trigger_dbt_run(job_id: int, job_description: str) -> int:
    data = {"cause": f"Airflow run - {job_description}"}
    url = f"{BASE_URL}/accounts/{account_id}/jobs/{job_id}/run/"
    print(f"Calling dbt cloud API with URL {url}")
    resp = requests.request(
        "POST",
        url,
        json=data,
        headers=headers,
    )

    if resp.status_code == 200:
        run_id = resp.json()["data"]["id"]
        print(resp.json())
        print(f"Triggered dbt cloud job run with ID: {run_id}")
        return run_id


def poll_dbt_run(run_id: int):
    if not run_id:
        raise Exception("Unable to get run id")
    current_time = int(time.time())
    timeout_time = current_time + timeout

    status = "running"

    while current_time <= timeout_time and status == "running":
        resp = requests.get(f"{BASE_URL}/accounts/{account_id}/runs/{run_id}/", headers=headers)

        print(f"Checking dbt cloud run {run_id} status: {status}")

        if resp.status_code == 200:
            payload = resp.json()["data"]
            print(payload)
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
        print("An exception occurred")
        print(e)
        raise
