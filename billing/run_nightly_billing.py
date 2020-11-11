import base64
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import logging
import os

from sqlalchemy import *
from sqlalchemy.sql import select, and_
import requests


def get_beginning_of_month(dt):
    return datetime(dt.year, dt.month, 1)


def main():
    blapi_token = os.getenv("BLAPI_TOKEN")
    blapi_url = os.getenv("BLAPI_URL")
    header = {"Authorization": f"Bearer {blapi_token}"}

    engine = create_engine(os.getenv("BLAPI_DATABASE_URL"))
    with engine.connect() as conn:
        now = datetime.now()
        end_date = datetime(now.year, now.month, now.day)
        start_date = end_date + relativedelta(days=-1)
        payload = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        subscriptions = Table(
            "subscriptions", MetaData(), autoload=True, autoload_with=conn
        )

        query = select(["*"]).where(
            and_(
                subscriptions.c.deleted_at == None,
                subscriptions.c.cloud_installation_id != None,
            )
        )

        errors = []

        for sub in conn.execute(query):
            retries = 0
            url = f"{blapi_url}/api/v1/customer/{sub['customer_id']}/subscriptions/{sub['id']}/invoice/build"

            while retries < 5:
                try:
                    retries += 1
                    resp = requests.post(url, json=payload, headers=header)
                    break
                except requests.exceptions.ConnectionError:
                    pass

            if resp.status_code != requests.codes.ok:
                message = f"Error building invoice for subscription {sub['id']} with dates {start_date}-{end_date}"
                logging.error(message)
                errors.append(message)

        if errors:
            raise Exception("\n".join(errors))


if __name__ == "__main__":
    main()
