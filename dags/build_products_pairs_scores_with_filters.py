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

build_products_pairs_scores_with_filters_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="15 1 * * 1",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def build_products_pairs_scores_with_filters(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_products_pairs_scores_with_filters'
    ods_table_name = 'ods_products_pairs_scores_with_filters'
    business_key = 'product_id, substitute'
    table_columns_comma_sep = 'product_id, substitute, naming_score, photo_score, tickets_score, attributes_score, ga_score, same_model_id, same_type, same_price_quartile, same_price_quartile_per_unit, same_brand, same_dep, same_subdep, same_class, same_subclass, actual_gamma, substitute_is_variant, families_intersection, load_dttm'
    mapping_select_to_target = '''
                            product_id, 
                            substitute, 
                            naming_score, 
                            photo_score, 
                            tickets_score, 
                            attributes_score, 
                            ga_score, 
                            case 
                                when same_model_id = 't' then '1'
                                else '0'
                            end as same_model_id, 
                            case 
                                when same_type = 't' then '1'
                                else '0'
                            end as same_type, 
                            case 
                                when same_price_quartile = 't' then '1'
                                else '0'
                            end as same_price_quartile, 
                            case 
                                when same_price_quartile_per_unit = 't' then '1'
                                else '0'
                            end as same_price_quartile_per_unit, 
                            case 
                                when same_brand = 't' then '1'
                                else '0'
                            end as same_brand, 
                            case 
                                when same_dep = 't' then '1'
                                else '0'
                            end as same_dep, 
                            case 
                                when same_subdep = 't' then '1'
                                else '0'
                            end as same_subdep, 
                            case 
                                when same_class = 't' then '1'
                                else '0'
                            end as same_class, 
                            case 
                                when same_subclass = 't' then '1'
                                else '0'
                            end as same_subclass, 
                            case 
                                when actual_gamma = 't' then '1'
                                else '0'
                            end as actual_gamma,
                            case 
                                when substitute_is_variant = 't' then '1'
                                else '0'
                            end as substitute_is_variant, 
                            case 
                                when families_intersection = 't' then '1'
                                else '0'
                            end as families_intersection,
                            load_dttm
        '''

    # ===================================================LOAD TABLE====================================================

    load_increment_fact_table(logger=logger,
                              database_name=database_name,
                              source_table_name=raw_table_name,
                              target_table_name=ods_table_name,
                              target_table_columns_comma_sep=table_columns_comma_sep,
                              mapping_select_to_target=mapping_select_to_target,
                              business_key=business_key)

    # ==================================================END OPERATOR===================================================


build_products_pairs_scores_with_filters = PythonOperator(
    task_id="build_products_pairs_scores_with_filters", python_callable=build_products_pairs_scores_with_filters, provide_context=True, dag=build_products_pairs_scores_with_filters_dag
)

build_products_pairs_scores_with_filters