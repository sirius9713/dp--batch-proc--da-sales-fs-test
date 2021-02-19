import os
import logging
import airflow
import pendulum
from airflow import DAG
from airflow.utils import dates as date
from datetime import timedelta, datetime
from airflow.models import BaseOperator, Pool
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
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

load_tickets_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="20 6 * * *",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def tickets_load(ds, **kwargs):
    from airflow_clickhouse_plugin import ClickHouseHook

    client = ClickHouseHook(clickhouse_conn_id='clickhouse_salesfs')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_tickets'
    ods_table_name = 'ods_tickets'
    table_columns_comma_sep = 'store, checkout_number, ticket_number, sell_date, ticket_time, transaction_type, item, purchase_price, total_cost, total_cost_wo_tax, item_qty, total_margin, num_acp, num_cde, created_dttm, load_dttm'
    table_columns_select = '''
                              store, 
                              checkout_number, 
                              ticket_number, 
                              sell_date, 
                              ticket_time, 
                              transaction_type, 
                              item, 
                              purchase_price, 
                              sum(total_cost) AS total_cost, 
                              sum(total_cost_wo_tax) AS total_cost_wo_tax, 
                              sum(item_qty) AS item_qty, 
                              sum(total_margin) AS total_margin, 
                              num_acp, 
                              num_cde, 
                              min(created_dttm) AS created_dttm, 
                              min(load_dttm) AS load_dttm
                          '''
    table_columns_group_by = 'store, checkout_number, ticket_number, sell_date, ticket_time, transaction_type, item, purchase_price, num_acp, num_cde'
    business_dttm = 'sell_date'

    # ===================================================QUERY INIT====================================================

    GET_MIN_MAX_DATES_SQL = """
                            SELECT 
                                toDate(min({business_dttm})), 
                                toDate(max({business_dttm})) 
                            FROM {database_name}.{raw_table_name}
                            """

    COUNT_SQL = """
        SELECT  toString({business_dttm}) AS partition_name, count(*) 
        FROM {database_name}.{table_name}
        WHERE {business_dttm} BETWEEN toDateTime('{start_date}') AND toDateTime('{end_date}')
        GROUP BY {business_dttm} 
        """

    DROP_PART_SQL = """ALTER TABLE {database_name}.{ods_table_name} DROP PARTITION '{key}'"""

    INSERT_SQL = """
        INSERT INTO {database_name}.{ods_table_name} ({table_columns_comma_sep})
            SELECT  {table_columns_select}
            FROM {database_name}.{raw_table_name}
            WHERE {business_dttm} in toDateTime('{key}')
            GROUP BY {table_columns_group_by}
        """

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    # =================================================QUERY EXECUTION=================================================

    logger.info("{}, start".format(datetime.now()))

    dates = client.run(GET_MIN_MAX_DATES_SQL.format(database_name=database_name,
                                                    raw_table_name=raw_table_name,
                                                    business_dttm=business_dttm))
    start_date = str(dates[0][0]) + ' 00:00:00'
    end_date = str(dates[0][1]) + ' 00:00:00'

    logger.info("{}, start_date: {}, end_date: {}".format(datetime.now(), start_date, end_date))

    ods_count = dict(client.run(COUNT_SQL.format(database_name=database_name,
                                                table_name=ods_table_name,
                                                business_dttm=business_dttm,
                                                start_date=start_date,
                                                end_date=end_date)))
    raw_count = dict(client.run(COUNT_SQL.format(database_name=database_name,
                                                table_name=raw_table_name,
                                                business_dttm=business_dttm,
                                                start_date=start_date,
                                                end_date=end_date)))

    keys = raw_count.copy().keys()
    for key in keys:
        try:
            if raw_count[key] == ods_count[key]:
                raw_count.pop(key)
                logger.info("{}, partition {} is ok".format(datetime.now(), key))
            else:
                logger.info("{}, partition {} to reload raw={}, ods={}".format(datetime.now(),
                                                                               key,
                                                                               raw_count[key],
                                                                               ods_count[key]))
        except Exception as e:
            logger.info("{}, partition {} is new".format(datetime.now(), key))
            continue

    logger.info("{}, reload {} partitions".format(datetime.now(), len(raw_count)))
    logger.info("QUERY: " + INSERT_SQL.format(database_name=database_name,
                                              ods_table_name=ods_table_name,
                                              table_columns_comma_sep=table_columns_comma_sep,
                                              raw_table_name=raw_table_name,
                                              business_dttm=business_dttm,
                                              key='',
                                              table_columns_select=table_columns_select,
                                              table_columns_group_by=table_columns_group_by))

    for key in raw_count:
        client.run(DROP_PART_SQL.format(database_name=database_name,
                                        ods_table_name=ods_table_name,
                                        key=key))
        logger.info("{}, partition {} is dropped".format(datetime.now(), key))
        client.run(INSERT_SQL.format(database_name=database_name,
                                     ods_table_name=ods_table_name,
                                     table_columns_comma_sep=table_columns_comma_sep,
                                     raw_table_name=raw_table_name,
                                     business_dttm=business_dttm,
                                     key=key,
                                     table_columns_select=table_columns_select,
                                     table_columns_group_by=table_columns_group_by))
        logger.info("{}, partition {} is uploaded".format(datetime.now(), key))

    logger.info("{0}, truncate {1}.{2}".format(datetime.now(), database_name, raw_table_name))
    client.run(TRUNCATE_SQL.format(database_name=database_name,
                                   table_name=raw_table_name))
    logger.info("{}, end".format(datetime.now()))


load_tickets = PythonOperator(
   task_id="load_tickets", python_callable=tickets_load, provide_context=True, dag=load_tickets_dag
)

load_tickets