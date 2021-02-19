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

build_rms_item_master_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="35 9 * * *",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def build_rms_item_master(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_rms_item_master'
    ods_table_name = 'ods_rms_item_master'
    table_columns_comma_sep = 'nk, item, item_number_type, format_id, prefix, item_parent, item_grandparent, pack_ind, item_level, tran_level, item_aggregate_ind, diff_1, diff_1_aggregate_ind, diff_2, diff_2_aggregate_ind, diff_3, diff_3_aggregate_ind, diff_4, diff_4_aggregate_ind, dept, class, subclass, status, item_desc, item_desc_secondary, short_desc, desc_up, primary_ref_item_ind, retail_zone_group_id, cost_zone_group_id, standard_uom, uom_conv_factor, package_size, package_uom, merchandise_ind, store_ord_mult, forecast_ind, original_retail, mfg_rec_retail, retail_label_type, retail_label_value, handling_temp, handling_sensitivity, catch_weight_ind, waste_type, waste_pct, default_waste_pct, const_dimen_ind, simple_pack_ind, contains_inner_ind, sellable_ind, orderable_ind, pack_type, order_as_type, comments, item_service_level, gift_wrap_ind, ship_alone_ind, create_datetime, last_update_id, last_update_datetime, check_uda_ind, item_xform_ind, inventory_ind, order_type, sale_type, deposit_item_type, container_item, deposit_in_price_per_uom, aip_case_type, banded_item_ind, catch_weight_type, perishable_ind, soh_inquiry_at_pack_ind, notional_pack_ind, catch_weight_uom, valid_from_dttm, created_dttm, updated_dttm,  is_actual, load_dttm'
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

    # ==================================================END OPERATOR===================================================


build_rms_item_master = PythonOperator(
    task_id="build_rms_item_master", python_callable=build_rms_item_master, provide_context=True, dag=build_rms_item_master_dag
)

build_rms_item_master