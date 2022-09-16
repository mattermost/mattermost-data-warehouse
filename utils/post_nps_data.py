import snowflake.connector
import requests
import os
from tabulate import tabulate
from pprint import pprint

def format_row(row):
    row = list(row)
    row[0] = row[0].replace('\n',' ').replace('"','')
    return tuple(row)

conn = snowflake.connector.connect(
    user = os.getenv("SNOWFLAKE_USER"),
    password = os.getenv("SNOWFLAKE_PASSWORD"),
    account = os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse = os.getenv("SNOWFLAKE_TRANSFORM_WAREHOUSE"),
    database = os.getenv("SNOWFLAKE_TRANSFORM_DATABASE"),
    schema = os.getenv("SNOWFLAKE_TRANSFORM_SCHEMA")
    )

try:
    cur = conn.cursor()

    sql = f'''select distinct feedback, subcategory as category, score, server_version, 
    case when sf.installation_id is null then 'Self-Hosted' 
    when sf.installation_id is not null then 'Cloud' end as INSTALLATION_TYPE,
    user_role, lsf.customer_name
    from ANALYTICS.MATTERMOST.NPS_USER_DAILY_SCORE nps
    left join mattermost.server_fact sf on nps.server_id = sf.server_id
    left join blp.license_server_fact lsf on nps.server_id = lsf.server_id 
    left join analytics.mattermost.nps_feedback_classification fc on nps.user_id = fc.user_id and nps.server_id = fc.server_id and nps.last_feedback_date = fc.last_feedback_date
    WHERE 
    nps.last_feedback_DATE = CURRENT_DATE - 1 and subcategory <> 'Invalid' and 
    feedback <> ''
    '''

    cur.execute(sql)
    out = cur.fetchall()
    out = tuple(map(lambda x: format_row(x) , out))
    out = tabulate(out, headers=['Feedback','Category','Score','Server Version','Installation Type','User Role','Customer Name'], tablefmt='github')
    print("Out length: ", len(out))
    payload='{"text": "%s", "channel": "mattermost-nps-feedback"}' % out
    nps_webhook_url = os.getenv("NPS_WEBHOOK_URL")
    print(nps_webhook_url)
    print(payload)
    response = requests.post(
            nps_webhook_url, data=payload.encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
    print(response.status_code)
    pprint(response)
    if response.status_code != 200:
        print('response is ',response.text)
        raise ValueError(
        'Request to Mattermost returned an error %s, the response is:\n%s'
        % (response.status_code, response.text)
    )

finally:
    cur.close()
    conn.close()