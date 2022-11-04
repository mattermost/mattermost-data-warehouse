from airflow import DAG

from operators.mattermost_operator import MattermostOperator
from datetime import datetime
MATTERMOST_CONN_ID = 'mattermost'


dag = DAG('mattermost_operator_dag', description='Mattermost Send Message DAG',
          start_date=datetime(2017, 3, 20), catchup=False)

with dag:
    mattermost = MattermostOperator(mattermost_conn_id=MATTERMOST_CONN_ID,
                                    text='Hello from mattermost operator\nCurrent run: {{ run_id }}',
                                    icon_url='https://mattermost.com/wp-content/uploads/2022/02/icon.png',
                                    task_id='notify')

mattermost