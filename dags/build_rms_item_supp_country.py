import os
import logging
import airflow
import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.clickhouse_hook import ClickHouseHook

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

build_rms_item_supp_country_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="35 9 * * *",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)

def build_rms_item_supp_country(ds, **kwargs):
    
    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_rms_item_supp_country'
    ods_table_name = 'ods_rms_item_supp_country'
    table_columns_comma_sep = 'nk, item, supplier, origin_country_id, unit_cost, lead_time, pickup_lead_time, supp_pack_size, inner_pack_size, round_lvl, round_to_inner_pct, round_to_case_pct, round_to_layer_pct, round_to_pallet_pct, min_order_qty, max_order_qty, packing_method, primary_supp_ind, primary_country_ind, default_uop, ti, hi, supp_hier_type_1, supp_hier_lvl_1, supp_hier_type_2, supp_hier_lvl_2, supp_hier_type_3, supp_hier_lvl_3, create_datetime, last_update_datetime, last_update_id, cost_uom, tolerance_type, max_tolerance, min_tolerance, negotiated_item_cost, extended_base_cost, inclusive_cost, base_cost, valid_from_dttm, created_dttm, updated_dttm,  is_actual, load_dttm'
    business_key = 'item, supplier, primary_supp_ind, primary_country_ind'
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


build_rms_item_supp_country = PythonOperator(
    task_id="build_rms_item_supp_country", python_callable=build_rms_item_supp_country, provide_context=True, dag=build_rms_item_supp_country_dag
)

build_rms_item_supp_country