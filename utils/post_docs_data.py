import snowflake.connector
import requests
import os
from tabulate import tabulate

def format_row(row):
    row = list(row)
    row[0] = row[0].replace('\n',' ').replace('"','')
    return tuple(row)

def post_docs():
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
        docs_webhook_url = os.getenv("DOCS_WEBHOOK_URL").strip('\n')
        sql = f'''
        select distinct
            coalesce(mm_docs.feedback, 'null') as feedback,
            mm_docs.context_page_path as path,
            mm_docs.rating as rating
        from 
            ANALYTICS.WEB.MATTERMOST_DOCS_FEEDBACK as mm_docs
        where
            mm_docs.timestamp::date >= current_date() - 7 and 
            mm_docs.timestamp::date <= current_date() - 1
        order by
            rating desc
        limit 500
        '''
        # airflow request run weekly 
        cur.execute(sql)
        out = cur.fetchall()
        if len(out) == 0:
            payload = '{"text": "There were no reviews this week.", "channel": "mattermost-documentation-feedback"}'
        else:
            out = tuple(map(lambda x: format_row(x) , out))
            out = tabulate(out, headers=['Feedback','Path','Rating'], tablefmt='github')
            payload='{"text": "%s", "channel": "mattermost-documentation-feedback"}' % out
        
        response = requests.post(
                docs_webhook_url, data=payload.encode('utf-8'),
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

if __name__ == '__main__':
    post_docs()