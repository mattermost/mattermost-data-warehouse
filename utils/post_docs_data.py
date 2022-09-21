import snowflake.connector
import requests
import os
from tabulate import tabulate

def format_row(row):
    row = list(row)
    row[0] = row[0].replace('\n',' ').replace('"','')
    return tuple(row)

conn = snowflake.connector.connect(
    user = # os.getenv("SNOWFLAKE_USER"),
    password = # os.getenv("SNOWFLAKE_PASSWORD"),
    account = # os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse = # os.getenv("SNOWFLAKE_TRANSFORM_WAREHOUSE"),
    database = # os.getenv("SNOWFLAKE_TRANSFORM_DATABASE"),
    schema = # os.getenv("SNOWFLAKE_TRANSFORM_SCHEMA")
    )

try:
    cur = conn.cursor()

    sql = f'''
    select distinct
        mm_docs.timestamp::date as date,
        mm_docs.rating as rating,
        mm_docs.feedback as feedback,
        mm_docs.context_page_path as path,
        mm_docs.anonymous_id as anon_id
    from 
        ANALYTICS.WEB.MATTERMOST_DOCS_FEEDBACK as mm_docs
    where
        date = current_date() - 1
    order by
        date desc
    limit 500
    '''

    cur.execute(sql)
    out = cur.fetchall()
    out = tuple(map(lambda x: format_row(x) , out))
    out = tabulate(out, headers=['Timestamp Date','User Id','Context Page Path','Rating','Feedback'], tablefmt='github')
    payload='{"text": "%s", "channel": "town-square"}' % out
    nps_webhook_url = "https://henryxuworkspace.cloud.mattermost.com/hooks/wdi7ajmz9tbxb8xqqcnxb15ter"
    print('Value of webhook - ', nps_webhook_url[-4:] + '-' + nps_webhook_url[:4])
    response = requests.post(
            nps_webhook_url, data=payload.encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
    if response.status_code != 200:
        print('response is ',response.text)
        raise ValueError(
        'Request to Mattermost returned an error %s, the response is:\n%s'
        % (response.status_code, response.text)
    )

finally:
    cur.close()
    conn.close()