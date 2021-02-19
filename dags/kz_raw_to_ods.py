import os
import pendulum
import logging
from airflow import DAG
from datetime import timedelta, datetime
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
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2020, 7, 10, tzinfo=LOCAL_TZ),
    'queue': MODULE_NAME,
    'concurrency': 10
}


load_kz_dag=DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="00 4 * * *", 
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)

def kz_load(ds, **kwargs):
    
    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_kz_order_src'
    ods_table_name = 'ods_kz_order_src'
    table_columns_comma_sep = 'order_create_ts, region_name, loc, order_num, lm_code, product, order_status, department_name, department, ordered_quantity, total_oper_quantity, price, payment_type, operation_type, device_type, delivery_type, reason, email, primary_phone, secondary_phone, type, line_id, front_app, order_channel, ship_to_country, ship_to_region, ship_to_district, ship_to_postal_code, ship_to_city, ship_to_street, ship_to_house, ship_to_building, ship_to_housing, ship_to_entrance, ship_to_flat, ship_to_floor, ship_to_gps_y, ship_to_gps_x, payment_state, load_dttm'
    business_key = 'order_num, product'
    business_dttm = 'order_create_ts'

    # ===================================================LOAD TABLE====================================================

    load_increment_fact_table(logger=logger,
                              database_name=database_name,
                              source_table_name=raw_table_name,
                              target_table_name=ods_table_name,
                              target_table_columns_comma_sep=table_columns_comma_sep,
                              business_key=business_key,
                              business_dttm=business_dttm)


    # =====================================================END OPERATOR================================================


load_kz = PythonOperator(
    task_id="load_kz", python_callable=kz_load, provide_context=True, dag=load_kz_dag
)

load_kz