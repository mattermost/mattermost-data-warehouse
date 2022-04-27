import snowflake.connector
import requests
from tabulate import tabulate

def format_row(row):
    row = list(row)
    row[3] = row[3].replace('\n',' ')
    return tuple(row)

conn = snowflake.connector.connect(
    user = SNOWFLAKE_USER,
    password = SNOWFLAKE_PASSWORD,
    account = SNOWFLAKE_ACCOUNT,
    warehouse = SNOWFLAKE_TRANSFORM_WAREHOUSE,
    database = SNOWFLAKE_TRANSFORM_DATABASE,
    schema = SNOWFLAKE_TRANSFORM_SCHEMA
            )

try:
    cur = conn.cursor()

    sql = f'''select distinct user_role, server_version, score, feedback, lsf.customer_name,
    --nps.server_id, nps.user_id, 
    case when sf.installation_id is null then 'Self-Hosted' 
    when sf.installation_id is not null then 'Cloud' end as INSTALLATION_TYPE
    from ANALYTICS.MATTERMOST.NPS_USER_DAILY_SCORE nps
    left join mattermost.server_fact sf on nps.server_id = sf.server_id
    left join blp.license_server_fact lsf on nps.server_id = lsf.server_id WHERE 
    last_feedback_DATE = CURRENT_DATE - 1 and 
    feedback <> ''
    '''

    cur.execute(sql)
    out = cur.fetchall()
    out = tuple(map(lambda x: format_row(x) , out))
    out = tabulate(out, headers=['User Role', 'Server Version', 'Score','Feedback', 'Customer Name', 'Installation Type'], tablefmt='github')

    webhook_url = ""
    payload='{"text": "%s", "channel": "mattermost-nps-feedback"}' % out

    response = requests.post(
            webhook_url, data=payload.encode('utf-8'),
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