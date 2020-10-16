import base64
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import logging
import os

from sqlalchemy import *
from sqlalchemy.sql import select, and_
import requests


def get_beginning_of_month(self, datetime: datetime) -> date:
    beginning_of_month = datetime.date()
    beginning_of_month.replace(day=1)
    return beginning_of_month


def __main__():
    blapi_token = os.getenv("BLAPI_TOKEN")
    blapi_url = os.getenv("BLAPI_URL")
    header = {"Authorization": f"Bearer {base64.b64encode(blapi_token)}"}

    engine = create_engine(os.getenv("BLAPI_DATABASE_URL"))
    with engine.connect() as conn:
        start_date = get_beginning_of_month(datetime.now())
        end_date = start_date + relativedelta(months=1)
        payload = {"start_date": start_date, "end_date": end_date}

        subscriptions = Table(
            "subscriptions", MetaData(), autoload=True, autoload_with=conn
        )

        query = select(subscriptions).where(
            and_(
                subscriptions.c.deleted_at != None,
                subscriptions.c.cloud_installation_id != None,
            )
        )

        errors = []

        logging.error(blapi_url)

        for sub in conn.execute(query):
            url = f"{blapi_url}/api/v1/customer/{sub['customer_id']}/subscriptions/{sub['id']}/invoice/build"
            resp = requests.post(url, data=data, headers=header)
            print(f"{resp.status_code} {url}")
            if resp.status_code != requests.codes.ok:
                message = f"Error building invoice for subscription {sub['id']} with dates {start_date}-{end_date}"
                logging.error(message)
                errors.append(message)

        if errors:
            raise Exception(errors.join("\n"))

