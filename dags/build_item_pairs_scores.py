import os
import logging
import airflow
import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator

# Set timezone
LOCAL_TZ = pendulum.timezone("Europe/Moscow")

# Batch processing module name
MODULE_NAME = 'sales-fs'

# Set dag id as module name + current filename
DAG_ID = MODULE_NAME + '__' + \
         os.path.basename(__file__).replace('.pyc', '').replace('.py', '')

args = {
    'owner': 'Zhilyakov Mikhail',
    'depends_on_past': False,
    'email': ['Mihail.Zhiljakov_ext@leroymerlin.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'start_date': airflow.utils.dates.days_ago(10),
    'queue': MODULE_NAME,
    'concurrency': 10
}

# schedule_interval задается в UTC. То есть надо указывать время в часах -3

build_item_pairs_scores_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="15 1 * * 2",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def build_item_pairs_scores(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_item_pairs_scores'
    ods_table_name = 'ods_item_pairs_scores'
    table_columns_comma_sep = 'item_a, item_b, pairs_count, conv, ticket_qty, load_dttm'
    business_key = 'item_a, item_b'

    # ===================================================LOAD TABLE====================================================

    load_increment_fact_table(logger=logger,
                              database_name=database_name,
                              source_table_name=raw_table_name,
                              target_table_name=ods_table_name,
                              target_table_columns_comma_sep=table_columns_comma_sep,
                              business_key=business_key)

    # ==================================================END OPERATOR===================================================


build_item_pairs_scores = PythonOperator(
    task_id="build_item_pairs_scores", python_callable=build_item_pairs_scores, provide_context=True, dag=build_item_pairs_scores_dag
)

build_item_pairs_scores