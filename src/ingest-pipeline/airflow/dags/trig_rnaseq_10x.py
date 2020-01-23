from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.operators.multi_dagrun import TriggerMultiDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook
from airflow.configuration import conf
from airflow.models import Variable
from datetime import datetime, timedelta
import pytz
from pprint import pprint
import os
import yaml
import json

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    'xcom_push': True
}

with DAG('trig_rnaseq_10x', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args) as dag:

 
#     def trigger_target(**kwargs):
#         ctx = kwargs['dag_run'].conf
#         run_id = kwargs['run_id']
#         print('run_id: ', run_id)
#         print('dag_run.conf:')
#         pprint(ctx)
#         print('kwargs:')
#         pprint(kwargs)


    def maybe_spawn_dags(**kwargs):
        """
        This is a generator which returns appropriate DagRunOrders
        """
        print('kwargs:')
        pprint(kwargs)
        metadata = kwargs['dag_run'].conf['metadata']
        auth_tok = kwargs['dag_run'].conf['auth_tok']
        assert 'components' in metadata, 'rnaseq_10x metadata with no components'
        payload = {'auth_tok':auth_tok,
                   'metadata':metadata,
                   'apply':'salmon_rnaseq_10x'}
        for elt in metadata['components']:
            pld = payload.copy()
            pld['component'] = elt
            yield DagRunOrder(payload=pld)
            break  # only emit the first trigger during debugging


    t_spawn_dag = TriggerMultiDagRunOperator(
        task_id="spawn_dag",
        trigger_dag_id="salmon_rnaseq_10x",  # Ensure this equals the dag_id of the DAG to trigger
        python_callable = maybe_spawn_dags,
        #conf={"message": "Hello World"},
        #provide_context = True
        )
 

#     t1 = PythonOperator(
#         task_id='trigger_target',
#         python_callable = trigger_target,
#         )


#     t_create_tmpdir = BashOperator(
#         task_id='create_temp_dir',
#         bash_command='mkdir ${AIRFLOW_HOME}/data/temp/{{run_id}}',
#         provide_context=True
#         )
# 
#  
#     t_send_status = PythonOperator(
#         task_id='send_status_msg',
#         python_callable=send_status_msg,
#         provide_context=True
#     )
 
  
    dag >> t_spawn_dag # >> t_create_tmpdir >> t_cleanup_tmpdir
