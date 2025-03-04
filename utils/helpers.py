from datetime import timedelta
from typing import List

import pandas as pd
import requests
from tabulate import tabulate


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def post_df_to_mattermost(url: str, channel: str, df: pd.DataFrame, headers: List[str], empty_data_message: str):
    if df is None or df.empty:
        response = requests.post(
            url,
            json={"text": empty_data_message, "channel": channel},
        )
    else:
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(lambda val: val.replace("\n", "\\n") if val and type(val) == str else val)

        msg = tabulate(df, headers=headers, tablefmt='github', showindex='never')
        response = requests.post(
            url,
            json={"text": msg, "channel": channel},
        )

    if response.status_code != 200:
        raise ValueError(f'Request to Mattermost returned {response.status_code}, the response is:\n{response.text}')
