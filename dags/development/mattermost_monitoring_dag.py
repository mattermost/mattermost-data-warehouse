from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from operators.mattermost_operator import MattermostWebhookOperator
from sensors.stitch_sensor_v2 import StitchIntegrationSensor
from datetime import datetime, timedelta

#TODO move to utils or reuse function created previously
def mattermost_alert(context):
    print("mattermost alert invoked")
    mattermost_webhook_url = BaseHook.get_connection("mattermost").host
    mattermost_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    print(mattermost_msg)
    failed_alert = MattermostWebhookOperator(
        task_id='fail_alert',
        http_conn_url=mattermost_webhook_url,
        message=mattermost_msg,
        channel='data-dev',
        username='airflow')
    return failed_alert.printme(context=context)

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': mattermost_alert,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'mattermost_sample_dag',
    default_args=default_args,
    start_date=datetime(2015, 12, 1),
    description='A DAG for monitoring mattermost data pipeline',
    schedule_interval='@daily',
    catchup=False,
    )

t1 = StitchIntegrationSensor(
    task_id='stitch_sensor',
    integration_name='stitch_sensor',
    dag=dag,
)
