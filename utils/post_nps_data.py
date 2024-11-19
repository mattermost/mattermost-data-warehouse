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
        select distinct
            feedback,
            subcategory as category,
            score,
            server_version,
            case
                when sf.installation_id is null then 'Self-Hosted'
                when sf.installation_id is not null then 'Cloud' end as INSTALLATION_TYPE,
            user_role,
            lsf.customer_name,
            lsf.license_email,
            coalesce(email, '') as email,
            sf.max_active_user_count as user_count
        from ANALYTICS.MATTERMOST.NPS_USER_DAILY_SCORE nps
        left join mattermost.server_fact sf on nps.server_id = sf.server_id
        left join blp.license_server_fact lsf on nps.server_id = lsf.server_id
        left join analytics.mattermost.nps_feedback_classification fc
            on nps.user_id = fc.user_id
               and nps.server_id = fc.server_id
               and nps.last_feedback_date = fc.last_feedback_date
        where
            nps.last_feedback_DATE = CURRENT_DATE - 1
            and subcategory <> 'Invalid'
            and feedback <> ''
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
                    'Category',
                    'Score',
                    'Server Version',
                    'Installation Type',
                    'User Role',
                    'Customer Name',
                    'License Email',
                    'Email',
                    'User Count',
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
