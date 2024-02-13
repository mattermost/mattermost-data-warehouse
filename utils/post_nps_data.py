import os

import requests
import snowflake.connector
from tabulate import tabulate


def format_row(row):
    row = list(row)
    row[0] = row[0].replace('\n', ' ').replace('"', '')
    return tuple(row)


def post_nps():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_TRANSFORM_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_TRANSFORM_DATABASE"),
        schema=os.getenv("SNOWFLAKE_TRANSFORM_SCHEMA"),
    )

    try:
        cur = conn.cursor()
        nps_webhook_url = os.getenv("NPS_WEBHOOK_URL")
        nps_webhook_url = nps_webhook_url.strip('\n')
        sql = '''
        select distinct nps.feedback as feedback
            , nps.score as score
            , nps.server_version as server_version
            , au.installation_type as installation_type
            , nps.user_role as user_role
            , coalesce(cc.company_name, shc.company_name) as company_name
            , coalesce(cc.customer_email, shc.customer_email) as customer_email
            , au.count_registered_active_users as registered_active_users
        from mart_product.fct_nps_feedback nps
        left join mart_product.fct_active_servers au on nps.server_id = au.server_id and nps.feedback_date = au.activity_date
        left join mart_product.dim_cloud_customers cc on nps.server_id = cc.server_id
        left join mart_product.dim_self_hosted_customers shc on nps.server_id = shc.server_id
        where nps.feedback_date = CURRENT_DATE - 1 and feedback <> ''
        '''

        cur.execute(sql)
        out = cur.fetchall()

        if len(out) == 0:
            payload = '{"text": "There were no reviews yesterday.", "channel": "mattermost-nps-feedback"}'
        else:
            out = tuple([format_row(x) for x in out])
            out = tabulate(
                out,
                headers=[
                    'Feedback',
                    'Score',
                    'Server Version',
                    'Installation Type',
                    'User Role',
                    'Company Name',
                    'Customer License Email',
                    'Registered Active Users',
                ],
                tablefmt='github',
            )
            payload = '{"text": "%s", "channel": "mattermost-nps-feedback"}' % out
        # TODO use mattermost operator for below post
        response = requests.post(
            nps_webhook_url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            raise ValueError(
                'Request to Mattermost returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
            )

    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    post_nps()
