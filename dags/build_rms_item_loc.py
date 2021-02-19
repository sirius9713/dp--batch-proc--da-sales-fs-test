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

build_rms_item_loc_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="0 9 * * *",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)

def build_rms_item_loc(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_rms_item_loc'
    ods_table_name = 'ods_rms_item_loc'
    table_columns_comma_sep = 'nk, item, loc, item_parent, item_grandparent, loc_type, unit_retail, regular_unit_retail, multi_units, multi_unit_retail, multi_selling_uom, selling_unit_retail, selling_uom, promo_retail, promo_selling_retail, promo_selling_uom, clear_ind, taxable_ind, local_item_desc, local_short_desc, ti, hi, store_ord_mult, status, status_update_date, daily_waste_pct, meas_of_each, meas_of_price, uom_of_price, primary_variant, primary_cost_pack, primary_supp, primary_cntry, receive_as_type, create_datetime, last_update_datetime, last_update_id, inbound_handling_days, source_method, source_wh, store_price_ind, rpm_ind, uin_type, uin_label, capture_time, ext_uin_ind, valid_from_dttm, created_dttm, updated_dttm, is_actual, load_dttm'
    business_key = 'item, loc'
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

    # ==================================================END OPERATOR===================================================


build_rms_item_loc = PythonOperator(
    task_id="build_rms_item_loc", python_callable=build_rms_item_loc, provide_context=True, dag=build_rms_item_loc_dag
)

build_rms_item_loc