from datetime import timedelta
from typing import List

import pandas as pd
import requests
from tabulate import tabulate


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def post_to_mattermost(url: str, channel: str, df: pd.DataFrame, headers: List[str]):
    msg = tabulate(df, headers=headers, tablefmt='github', showindex='never')
    response = requests.post(
        url,
        json={"text": msg, "channel": channel},
    )

    if response.status_code != 200:
        raise ValueError(f'Request to Mattermost returned {response.status_code}, the response is:\n{response.text}')
