import os
import airflow
import pendulum
import logging
from datetime import date
from airflow import DAG
#from airflow.utils import dates as date
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook



# Set timezone
LOCAL_TZ = pendulum.timezone("Europe/Moscow")

# Batch processing module name
MODULE_NAME = 'sales-fs'

# Set dag id as module name + current filename
DAG_ID = MODULE_NAME + '__' + \
    os.path.basename(__file__).replace('.pyc', '').replace('.py', '')

args = {
    'owner': 'Danilo Chadzhenovich',
    'depends_on_past': False,
    'email': ['Danilo.Chadzhenovich@leroymerlin.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(10),
    'queue': MODULE_NAME,
    'concurrency': 10
}


load_tickets_features_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="30 6 * * 2",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def build_item_qty_to_drop_outlier(ds, **kwargs):

    # from sf_api.run_clickhouse import run_clickhouse_query

    gp_client = PostgresHook(postgres_conn_id='gp_salesfs')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    database_name = 'subdonors_marts'
    work_table_name = 'work_item_qty_to_drop_outlier'

    # ===================================================QUERY INIT====================================================

    TRUNCATE_SQL = """ TRUNCATE TABLE {database_name}.{table_name}"""

    BUILD_WORK_SQL = """INSERT INTO {database_name}.{table_name} (item, item_qty_max_offline, item_qty_max_online)
                        SELECT 
                            item, 
                            CASE
                                WHEN ticket_qty_offline >= ticket_qty_online AND ticket_qty_offline > 10 THEN item_qty_95_raw_offline
                                WHEN ticket_qty_offline >= ticket_qty_online THEN item_qty_max_raw_offline
                                WHEN ticket_qty_offline < ticket_qty_online AND ticket_qty_online > 10 THEN item_qty_95_raw_online
                                WHEN ticket_qty_offline < ticket_qty_online THEN item_qty_max_raw_online
                            END AS item_qty_max_offline, 
                            CASE
                                WHEN ticket_qty_online > 10 THEN item_qty_95_raw_online
                                WHEN ticket_qty_online >= ticket_qty_offline THEN item_qty_max_raw_online
                                WHEN ticket_qty_online < ticket_qty_offline AND ticket_qty_offline > 10 THEN item_qty_95_raw_offline
                                WHEN ticket_qty_online < ticket_qty_offline THEN item_qty_max_raw_offline
                            END AS item_qty_max_online
                        FROM 
                        ( 
                            SELECT 
                               COALESCE(ot.item, vmood.item) AS item,
                               COALESCE(item_qty_max_raw_offline, 0) AS item_qty_max_raw_offline, 
                               COALESCE(item_qty_95_raw_offline, 0) AS item_qty_95_raw_offline,
                               COALESCE(ticket_qty_offline, 0) AS ticket_qty_offline,
                               COALESCE(item_qty_max_raw_online, 0) AS item_qty_max_raw_online,
                               COALESCE(item_qty_95_raw_online, 0) AS item_qty_95_raw_online,
                               COALESCE(ticket_qty_online, 0) AS ticket_qty_online
                            FROM 
                            (
                                SELECT 
                                    item, 
                                    max(item_qty) AS item_qty_max_raw_offline, 
                                    (PERCENTILE_CONT (0.95) within group (order by item_qty))*1.5 AS item_qty_95_raw_offline, 
                                    count() AS ticket_qty_offline
                                FROM {database_name}.ods_tickets ot 
                                WHERE 
                                    store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                                GROUP BY item
                            ) ot
                            FULL OUTER JOIN 
                            (
                                SELECT 
                                    item::integer AS item, 
                                    max(item_qty) AS item_qty_max_raw_online, 
                                    PERCENTILE_CONT (0.95) within group (order by item_qty) AS item_qty_95_raw_online, 
                                    count() AS ticket_qty_online
                                FROM {database_name}.view_mart_online_orders_365d vmood 
                                WHERE 
                                    store::integer in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                                GROUP BY item
                            ) vmood 
                            ON ot.item = vmood.item
                        ) b
                    """

    # =================================================QUERY EXECUTION=================================================

    logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                               database_name,
                                               work_table_name))

    logger.info("{0}, insert data into {1}.{2}".format(datetime.now(),
                                                       database_name,
                                                       work_table_name))

    # run_clickhouse_query(logger=logger,
    #                      clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
    #                                                                 table_name=work_table_name),
    #                                             BUILD_WORK_SQL.format(database_name=database_name,
    #                                                                   table_name=work_table_name)])

    gp_client.run(TRUNCATE_SQL.format(database_name=database_name, table_name=work_table_name))

    gp_client.run(BUILD_WORK_SQL.format(database_name=database_name, table_name=work_table_name))

    logger.info("{0}, END".format(datetime.now()))

    # ==================================================END OPERATOR===================================================


def load_tickets(ds, **kwargs):

    # from sf_api.run_clickhouse import run_clickhouse_query

    gp_client = PostgresHook(postgres_conn_id='gp_salesfs')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'subdonors_marts'
    tickets_view_name = 'view_ods_tickets'
    first_sell_date_table_name = 'first_sell_date'
    agg_day_store_table_name = 'work_tickets_agg_day_store_item'
    upsampled_tmp_table_name = 'work_tickets_agg_day_store_item_upsampled_tmp'
    upsampled_table_name = 'work_tickets_agg_day_store_item_upsampled'
    load_period = 7

    # =================================================================================================================

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    DROP_PART_SQL = """ALTER TABLE {database_name}.{table_name} DROP PARTITION '{sell_date}'"""

    # Список уникальных магазинов
    # UNIQUE_STORES_SQL = """
    #                 SELECT DISTINCT store
    #                 FROM (
    #                       SELECT store
    #                       FROM {database_name}.{tickets_view_name}
    #                       UNION ALL
    #                       SELECT store
    #                       FROM {database_name}.{upsampled_table_name}
    #                     )
    #                 ORDER BY store
    #             """.format(database_name=database_name,
    #                        tickets_view_name=tickets_view_name,
    #                        upsampled_table_name=upsampled_table_name)

    #stores = [store[0] for store in run_clickhouse_query(clickhouse_query_list=[UNIQUE_STORES_SQL])]
    stores = [2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 171, 176, 162]

    start_date = date.today() - timedelta(days=load_period)
    end_date = date.today()

    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days + 1)):
            yield start_date + timedelta(n)

    # work_tickets_agg_day_store_item -- агрегация "день, артикул, магазин"
    # toString(item) not like '49%' -- отсекаем "доставку"
    INSERT_WORK_TICKETS_AGG_DAY_STORE_ITEM_SQL = """
                    INSERT INTO {database_name}.{agg_day_store_table_name}
                    SELECT
                        sell_date,
                        store,
                        item,
                        tickets_qty,
                        total_cost,
                        item_qty,
                        round(total_cost/purchase_price) as purchase_price
                    FROM
                    (
                        SELECT 
                            sell_date,
                            store,
                            item,
                            count() as tickets_qty,
                            sum(total_cost) as total_cost,
                            sum(item_qty_processed) as item_qty,
                            sum(item_qty_raw) as purchase_price
                        FROM {database_name}.{tickets_view_name}
                        WHERE store::integer in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 171, 176, 162)
                          and item::text not like '49%'
                          -- and sell_date >= today() - {load_period}
                          and sell_date = '{sell_date}'::timestamp
                        GROUP BY 
                            sell_date, 
                            store, 
                            item,
                            item_qty_raw
                    ) b
                    """

    # Запрос для отчистки "календаря" от дней по артикулу, которые были до первой продажи артикула
    INSERT_FIRST_SELL_DATE_SQL = """
                        INSERT INTO {database_name}.{first_sell_date_table_name} (item, item_first_sale_date)
                        SELECT 
                            item, 
                            least(sell_date_min_offline, sell_date_min_online, sell_date_min_datablender) as item_first_sale_date
                        FROM (
                            SELECT 
                                COALESCE(ms_offline.item, COALESCE(ms_online.item, 0)) as item, 
                                --COALESCE(ms_offline.item, COALESCE(ms_online.item, COALESCE(ofsdd.item, 0))) as item,
                                CASE
                                    WHEN sell_date_min_offline IS NULL OR sell_date_min_offline = '1970-01-01 03:00:00'::timestamp
                                    THEN '2040-01-01 00:00:00'::timestamp
                                    ELSE sell_date_min_offline
                                END AS sell_date_min_offline,
                                CASE
                                    WHEN sell_date_min_online IS NULL OR sell_date_min_online = '1970-01-01 03:00:00'::timestamp
                                    THEN '2040-01-01 00:00:00'::timestamp
                                    ELSE sell_date_min_online
                                END AS sell_date_min_online,
                                '2040-01-01 00:00:00'::timestamp as sell_date_min_datablender
                                -- CASE
                                --     WHEN sell_date_min_datablender IS NULL OR sell_date_min_datablender = '1970-01-01 03:00:00'::timestamp
                                --     THEN '2040-01-01 00:00:00'::timestamp
                                --     ELSE sell_date_min_datablender
                                -- END AS sell_date_min_datablender
                            FROM (
                                SELECT 
                                    item::integer, 
                                    min(sell_date) as sell_date_min_offline
                                FROM {database_name}.{tickets_view_name}
                                WHERE store::integer in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                                GROUP BY item
                            ) ms_offline
                            FULL OUTER JOIN (
                                SELECT 
                                    item::integer as item, 
                                    min(sell_date) as sell_date_min_online
                                FROM {database_name}.view_mart_online_orders_365d
                                WHERE item::integer > 0
                                GROUP BY item
                            ) ms_online 
                            ON ms_offline.item = ms_online.item
                            -- FULL outer join (
                            --     SELECT item, min(sell_date) as sell_date_min_datablender
                            --     FROM {database_name}.ods_first_sale_date_datablender
                            --     WHERE store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                            --     GROUP BY item
                            -- ) ofsdd 
                            -- ON ms_offline.item = ofsdd.item
                        ) b
                        WHERE item > 0
                        """

    # Построение "календаря по продажам" на каждый день для отношений "магазин-артикул"
    INSERT_WORK_TICKETS_AGG_UPSAMPLED_TMP_SQL = """
                        INSERT INTO {database_name}.{upsampled_tmp_table_name}
                        SELECT 
                            sell_date, 
                            store, 
                            item, 
                            tickets_qty, 
                            total_cost, 
                            item_qty, 
                            purchase_price
                        FROM 
                        (
                            SELECT 
                                items.sell_date, 
                                items.store, 
                                items.item, 
                                tickets_qty, 
                                total_cost, 
                                item_qty, 
                                purchase_price
                            FROM 
                            (-- отношение "артикул-магазин-день" 
                                SELECT
                                    sell_date, 
                                    item, 
                                    items.tmp, 
                                    store
                                FROM 
                                (
                                    SELECT 
                                        DISTINCT
                                        item,
                                        'tmp' as tmp,
                                        store
                                    FROM {database_name}.{agg_day_store_table_name}
                                    WHERE store::integer in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                                ) items
                                LEFT JOIN 
                                (
                                    SELECT 
                                        'tmp' as tmp,
                                        --Для загрузки начального решения надо раскомментить строку ниже и убрать FROM
                                        '{sell_date}'::timestamp as sell_date
                                        -- today() - {load_period} + number as sell_date
                                    -- FROM numbers({load_period})
                                ) dd
                                ON items.tmp = dd.tmp
                            ) items
                            LEFT JOIN 
                            (-- реальные продажи
                                SELECT 
                                    sell_date, 
                                    item, 
                                    store, 
                                    tickets_qty, 
                                    total_cost, 
                                    item_qty, 
                                    purchase_price
                                FROM {database_name}.{agg_day_store_table_name}
                                WHERE 
                                -- Для загрузки начального решения надо раскомментить строку ниже
                                sell_date = '{sell_date}'::timestamp
                                -- sell_date <= today()
                                AND store::integer in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                            ) as tadi
                            ON items.sell_date = tadi.sell_date
                            AND items.item = tadi.item
                            AND items.store = tadi.store
                        ) as tick      
                            """

    # Добавляем к "календарю" отношения "магазин-артикул-день" цену:
    # Если в конкретный день были продажи артикула в магазине, берем цену
    # Если в конкретный день продаж не было, то рассчитываем цену исходя из продаж в прошлом
    # Если продаж в прошлом не было, рассчитываем цену исходя из будущих продаж относительно рассчитываемого дня
    # Иначе задаем цену, равной -1
    INSERT_WORK_TICKETS_AGG_UPSAMPLED_SQL = """
                    INSERT INTO {database_name}.{upsampled_table_name}
                    SELECT 
                        sell_date, 
                        store, 
                        tads.item, 
                        tickets_qty, 
                        total_cost, 
                        item_qty, 
                        purchase_price
                    FROM 
                    (
                        SELECT 
                            wtadsiu.sell_date, 
                            wtadsiu.store, 
                            wtadsiu.item, 
                            tickets_qty, 
                            total_cost, 
                            item_qty, 
                            CASE 
                                WHEN 
                                    (purchase_price_origin = 0 OR purchase_price_origin = double precision 'NaN' OR purchase_price_origin IS NULL) 
                                    AND purchase_price_past > 0
                                THEN purchase_price_past
                                WHEN 
                                    (purchase_price_origin = 0 OR purchase_price_origin = double precision 'NaN' OR purchase_price_origin IS NULL)
                                    AND (purchase_price_past = 0 OR purchase_price_past = double precision 'NaN' OR purchase_price_past IS NULL) 
                                    AND purchase_price_future > 0 
                                THEN purchase_price_future 
                                WHEN 
                                    (purchase_price_origin = 0 OR purchase_price_origin = double precision 'NaN' OR purchase_price_origin IS NULL)
                                    AND (purchase_price_past = 0 OR purchase_price_past = double precision 'NaN' OR purchase_price_past IS NULL) 
                                    AND (purchase_price_future = 0 OR purchase_price_future = double precision 'NaN' OR purchase_price_future IS NULL) 
                                THEN -1
                                ELSE purchase_price_origin
                            END AS purchase_price
                        FROM
                        (
                                SELECT 
                                    wtadsiu.sell_date, 
                                    wtadsiu.store, 
                                    wtadsiu.item, 
                                    tickets_qty, 
                                    total_cost, 
                                    item_qty,
                                    purchase_price as purchase_price_origin,
                                    purchase_price_past
                                FROM 
                                (
                                    SELECT 
                                        sell_date, 
                                        store, 
                                        item, 
                                        tickets_qty, 
                                        total_cost, 
                                        item_qty, 
                                        purchase_price
                                    FROM 
                                        {database_name}.{upsampled_tmp_table_name} wtadsiu
                                    WHERE sell_date = '{sell_date}'::timestamp
                                    AND store::integer IN (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                                ) wtadsiu
                                LEFT join lateral 
                                (
                                    SELECT 
                                        sell_date, 
                                        store, 
                                        item,
                                        purchase_price as purchase_price_past
                                    FROM 
                                        {database_name}.{upsampled_tmp_table_name} wtadsiu
                                    WHERE 
                                        purchase_price > 0
                                        AND store::integer IN (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                                        AND sell_date < '{sell_date}'::timestamp
                                ) as wtadsiu_past 
                                ON wtadsiu.store = wtadsiu_past.store 
                                    AND wtadsiu.item = wtadsiu_past.item 
                                    AND wtadsiu.sell_date > wtadsiu_past.sell_date
                        ) AS wtadsiu
                        LEFT join lateral 
                        (
                            SELECT
                                sell_date,
                                store,
                                item,
                                purchase_price as purchase_price_future
                            FROM {database_name}.{upsampled_tmp_table_name} wtadsiu
                            WHERE 
                                purchase_price > 0
                                and store::integer in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                                and sell_date > '{sell_date}'::timestamp
                        ) as wtadsiu_future 
                        ON wtadsiu.store = wtadsiu_future.store 
                            and wtadsiu.item = wtadsiu_future.item 
                            and wtadsiu.sell_date < wtadsiu_future.sell_date
                    ) tads
                    INNER JOIN {database_name}.{first_sell_date_table_name} stat
                        ON tads.item = stat.item 
                    WHERE tads.sell_date >= stat.item_first_sale_date
                        """

    # =================================================QUERY EXECUTION=================================================

    logger.info("{}, start".format(datetime.now()))

    logger.info(
        "{current_datetime}, build {database_name}.{first_sell_date_table_name}".format(current_datetime=datetime.now(),
                                                                                        database_name=database_name,
                                                                                        first_sell_date_table_name=first_sell_date_table_name))

    # run_clickhouse_query(logger=logger,
    #                      clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
    #                                                                 table_name=first_sell_date_table_name),
    #                                             INSERT_FIRST_SELL_DATE_SQL.format(database_name=database_name,
    #                                                                               first_sell_date_table_name=first_sell_date_table_name,
    #                                                                               tickets_view_name=tickets_view_name)
    #                                             ])

    gp_client.run(TRUNCATE_SQL.format(database_name=database_name, table_name=first_sell_date_table_name))

    gp_client.run(INSERT_FIRST_SELL_DATE_SQL.format(database_name=database_name, first_sell_date_table_name=first_sell_date_table_name, tickets_view_name=tickets_view_name))

    # for sell_date in daterange(start_date, end_date):
    #     run_clickhouse_query(logger=logger,
    #                          clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
    #                                                                      table_name=agg_day_store_table_name,
    #                                                                      sell_date=sell_date.strftime("%Y%m%d"))
    #                                                 ])
    #     logger.info(
    #         "{current_datetime}, {database_name}.{agg_day_store_table_name} partition {partition_date} is dropped".format(current_datetime=datetime.now(),
    #                                                                                                                       database_name=database_name,
    #                                                                                                                       agg_day_store_table_name=agg_day_store_table_name,
    #                                                                                                                       partition_date=sell_date))

    #     for store in stores:
    #         run_clickhouse_query(logger=logger,
    #                              clickhouse_query_list=[INSERT_WORK_TICKETS_AGG_DAY_STORE_ITEM_SQL.format(database_name=database_name,
    #                                                                                                       agg_day_store_table_name=agg_day_store_table_name,
    #                                                                                                       tickets_view_name=tickets_view_name,
    #                                                                                                       store_id=store,
    #                                                                                                       load_period=load_period,
    #                                                                                                       sell_date=sell_date)
    #                                                     ])

    for sell_date in daterange(start_date, end_date):
        gp_client.run(INSERT_WORK_TICKETS_AGG_DAY_STORE_ITEM_SQL.format(database_name=database_name,
                                                                        agg_day_store_table_name=agg_day_store_table_name,
                                                                        tickets_view_name=tickets_view_name,
                                                                        load_period=load_period,
                                                                        sell_date=sell_date))

    # for sell_date in daterange(start_date, end_date):
    #     run_clickhouse_query(logger=logger,
    #                          clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
    #                                                                      table_name=upsampled_tmp_table_name,
    #                                                                      sell_date=sell_date.strftime("%Y%m%d"))
    #                                                 ])
    #     logger.info("{current_datetime}, {database_name}.{upsampled_tmp_table_name} partition {partition_date} is dropped".format(current_datetime=datetime.now(),
    #                                                                                                                               database_name=database_name,
    #                                                                                                                               upsampled_tmp_table_name=upsampled_tmp_table_name,
    #                                                                                                                               partition_date=sell_date))

    #     for store in stores:

    #         run_clickhouse_query(logger=logger,
    #                              clickhouse_query_list=[INSERT_WORK_TICKETS_AGG_UPSAMPLED_TMP_SQL.format(database_name=database_name,
    #                                                                                                      upsampled_tmp_table_name=upsampled_tmp_table_name,
    #                                                                                                      agg_day_store_table_name=agg_day_store_table_name,
    #                                                                                                      store_id=store,
    #                                                                                                      load_period=load_period,
    #                                                                                                      sell_date=sell_date)
    #                                                     ])

    for sell_date in daterange(start_date, end_date):
        gp_client.run(INSERT_WORK_TICKETS_AGG_UPSAMPLED_TMP_SQL.format(database_name=database_name,
                                                                       upsampled_tmp_table_name=upsampled_tmp_table_name,
                                                                       agg_day_store_table_name=agg_day_store_table_name,
                                                                       load_period=load_period,
                                                                       sell_date=sell_date))

    # for sell_date in daterange(start_date, end_date):
    #     run_clickhouse_query(logger=logger,
    #                          clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
    #                                                                      table_name=upsampled_table_name,
    #                                                                      sell_date=sell_date.strftime("%Y%m%d"))
    #                                                 ])
    #     logger.info(
    #         "{current_datetime}, {database_name}.{upsampled_table_name} partition {partition_date} is dropped".format(current_datetime=datetime.now(),
    #                                                                                                                   database_name=database_name,
    #                                                                                                                   upsampled_table_name=upsampled_table_name,
    #                                                                                                                   partition_date=sell_date))
    #     logger.info("{current_datetime}, {database_name}.{upsampled_table_name} loading sell_date = {sell_date}".format(current_datetime=datetime.now(),
    #                                                                                                                     database_name=database_name,
    #                                                                                                                     upsampled_table_name=upsampled_table_name,
    #                                                                                                                     sell_date=sell_date))
    #     for store in stores:
    #         run_clickhouse_query(logger=logger,
    #                              clickhouse_query_list=[INSERT_WORK_TICKETS_AGG_UPSAMPLED_SQL.format(database_name=database_name,
    #                                                                                                  upsampled_table_name=upsampled_table_name,
    #                                                                                                  upsampled_tmp_table_name=upsampled_tmp_table_name,
    #                                                                                                  first_sell_date_table_name=first_sell_date_table_name,
    #                                                                                                  store_id=store,
    #                                                                                                  sell_date=sell_date)
    #                                                     ])

    for sell_date in daterange(start_date, end_date):

        logger.info("{current_datetime}, {database_name}.{upsampled_table_name} loading sell_date = {sell_date}".format(current_datetime=datetime.now(),
                                                                                                                        database_name=database_name,
                                                                                                                        upsampled_table_name=upsampled_table_name,
                                                                                                                        sell_date=sell_date))


        gp_client.run(INSERT_WORK_TICKETS_AGG_UPSAMPLED_SQL.format(database_name=database_name,
                                                                   upsampled_table_name=upsampled_table_name,
                                                                   upsampled_tmp_table_name=upsampled_tmp_table_name,
                                                                   first_sell_date_table_name=first_sell_date_table_name,
                                                                   sell_date=sell_date))

    logger.info("{}, end".format(datetime.now()))

    # ==================================================END OPERATOR===================================================





build_item_qty_to_drop_outlier = PythonOperator(
    task_id="build_item_qty_to_drop_outlier",
    python_callable=build_item_qty_to_drop_outlier,
    provide_context=True,
    dag=load_tickets_features_dag
)


load_tickets = PythonOperator(
    task_id="load_tickets",
    python_callable=load_tickets,
    provide_context=True,
    dag=load_tickets_features_dag
)

build_item_qty_to_drop_outlier >> load_tickets