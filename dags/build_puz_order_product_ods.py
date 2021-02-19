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
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    'start_date': airflow.utils.dates.days_ago(1),
    'queue': MODULE_NAME,
    'concurrency': 10
}


build_puz_order_product_ods_dag=DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="00 5 * * *", 
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)

def build_puz_order_product_ods(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_puz_order_product'
    ods_table_name = 'ods_puz_order_product'
    table_columns_comma_sep = 'nk, id, order_id, ordernumber, productnumber, name, section_externalid, subsection_externalid, type_externalid, subtype_externalid, stocks_site, stocks_pyxis, stocks_rms, stocks_wms, measure_name, weight, extrabig, extralengthy, extravolume, longtail, quantity, quantityordered, quantitycollected, quantitydelivered, quantityshippedmanual, quantityshipped, shippeddate, stockreturndate, stockreturnquantity, price, "sum", updatedat, versiondata, valid_from_dttm, valid_to_dttm, created_dttm, load_dttm'
    create_nk_field ="concat(toString(id), '|',toString(order_id),'|',productnumber,'|',toString(parseDateTimeBestEffort(updatedat))) AS nk"
    business_key = 'order_id, productnumber'
    business_dttm = 'created_dttm'

    # ===================================================LOAD TABLE====================================================

    load_increment_fact_table(logger=logger,
                              database_name=database_name,
                              source_table_name=raw_table_name,
                              target_table_name=ods_table_name,
                              target_table_columns_comma_sep=table_columns_comma_sep,
                              business_key=business_key,
                              business_dttm=business_dttm,
                              create_nk_field=create_nk_field)

    # ==================================================END OPERATOR===================================================


build_puz_order_product_ods = PythonOperator(
    task_id="build_puz_order_product_ods", python_callable=build_puz_order_product_ods, provide_context=True, dag=build_puz_order_product_ods_dag
)

build_puz_order_product_ods_dag