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
    'start_date': airflow.utils.dates.days_ago(1),
    'queue': MODULE_NAME,
    'concurrency': 10
}

build_rms_xxlm_path_version_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="50 9 * * *",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def build_rms_xxlm_path_version(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_rms_xxlm_path_version'
    ods_table_name = 'ods_rms_xxlm_path_version'
    table_columns_comma_sep = 'nk, id, activate_date, active_ind, fact_lead_time, last_review_date, cd_type, cd_wh, control_co, review_cycle, mon_ind, tue_ind, wed_ind, thu_ind, fri_ind, sat_ind, sun_ind, d_mon_ind, d_tue_ind, d_wed_ind, d_thu_ind, d_fri_ind, d_sat_ind, d_sun_ind, attribute1, attribute2, attribute3, attribute4, attribute5, attribute6, attribute7, attribute8, attribute9, attribute10, attribute11, attribute12, attribute13, attribute14, attribute15, create_id, create_datetime, last_update_id, last_update_datetime, transp_lead_time, franco_sum, lor_wght, lor_wght_plt, valid_from_dttm, created_dttm, updated_dttm,  is_actual, load_dttm'
    business_key = 'nk'
    business_dttm = 'created_dttm'
    is_act_field = 'is_actual'

    # ===================================================LOAD TABLE====================================================

    load_increment_fact_table(logger=logger,
                              database_name=database_name,
                              source_table_name=raw_table_name,
                              target_table_name=ods_table_name,
                              target_table_columns_comma_sep=table_columns_comma_sep,
                              business_key=business_key,
                              business_dttm=business_dttm,
                              is_act_field=is_act_field)


    # =====================================================END OPERATOR================================================


build_rms_xxlm_path_version = PythonOperator(
    task_id="build_rms_xxlm_path_version", python_callable=build_rms_xxlm_path_version, provide_context=True, dag=build_rms_xxlm_path_version_dag
)

build_rms_xxlm_path_version