import os
import airflow
import pendulum
import logging
from datetime import date
from airflow import DAG
#from airflow.utils import dates as date
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
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(10),
    'queue': MODULE_NAME,
    'concurrency': 10
}


load_tickets_features_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="50 7 * * 2",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


p_current_year_week_features_left = 8
p_current_year_week_features_right = 10
p_last_year_week_features_left = 12
p_last_year_week_features_right = 12
p_week_cnt = 7  # кол-во расчетных недель

p_database_name = 'salesfs'
p_dict_store_table_name = 'dict_stores'
p_dict_week_exception_table_name = 'dict_week_exception'
p_stock_table_name = 'ods_stock_history'
p_upsampled_table_name = 'work_tickets_agg_day_store_item_upsampled'
p_agg_tickets_by_store_table_name = 'mart_agg_tickets_by_store_dev'
p_avg_tickets_by_week_table_name = 'mart_avg_tickets_by_week_dev'
# изменить в DDL вьюх имена таблиц
p_avg_tickets_by_week_view_name = 'view_mart_avg_tickets_by_week_dev'
p_sum_tickets_by_week_table_name = 'mart_sum_tickets_by_week_dev'
p_agg_stock_by_store_table_name = 'mart_agg_stock_by_store'
p_agg_stock_by_week_table_name = 'mart_agg_stock_by_week'
p_avg_week_tickets_by_class_table_name = 'mart_avg_week_tickets_by_class'
p_avg_week_tickets_by_subclass_table_name = 'mart_avg_week_tickets_by_subclass'
p_avg_week_tickets_by_model_table_name = 'mart_avg_week_tickets_by_model'
p_avg_week_tickets_by_class_view_name = 'view_mart_avg_week_tickets_by_class'
p_avg_week_tickets_by_subclass_view_name = 'view_mart_avg_week_tickets_by_subclass'
p_avg_week_tickets_by_model_view_name = 'view_mart_avg_week_tickets_by_model'
p_agg_tickets_attr_by_week_table_name = 'mart_agg_tickets_attr_by_week_dev'
# изменить в DDL вьюх имена таблиц
p_agg_tickets_attr_by_week_view_name = 'view_mart_agg_tickets_attr_by_week_dev'
p_agg_tickets_attr_by_store_table_name = 'mart_agg_tickets_attr_by_store_dev'
# изменить в DDL вьюх имена таблиц
p_agg_tickets_attr_by_store_view_name = 'view_mart_agg_tickets_attr_by_store_dev'
p_ticket_features_by_week_table_name = 'mart_ticket_features_by_week_dev'
p_ticket_features_by_year_table_name = 'mart_ticket_features_by_year_dev'
p_ticket_features_table_name = 'mart_ticket_features_dev'
p_ticket_features_by_store_week_table_name = 'mart_ticket_features_by_store_week_dev'
p_ticket_features_by_store_year_table_name = 'mart_ticket_features_by_store_year_dev'
p_ticket_features_by_store_table_name = 'mart_ticket_features_by_store_dev'
p_item_attr_table_name = 'mart_item_attr'
p_item_master_table_name = 'ods_rms_item_master'
p_subclass_table_name = 'ods_rms_subclass'
p_products_info_table_name = 'ods_products_info'
p_uda_item_date_view_name = 'view_ods_rms_uda_item_date'
p_agg_tickets_attr_by_week_max_val_table_name = 'work_agg_tickets_attr_by_week_max_val'
p_agg_tickets_attr_by_store_max_val_table_name = 'work_agg_tickets_attr_by_store_max_val'
p_calendar_table_name = 'work_calendar_for_tickets'


def build_item_qty_to_drop_outlier(ds, **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    database_name = 'salesfs'
    work_table_name = 'work_item_qty_to_drop_outlier'

    # ===================================================QUERY INIT====================================================

    TRUNCATE_SQL = """ TRUNCATE TABLE IF EXISTS {database_name}.{table_name}"""

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
                        FROM ( 
                         SELECT 
                            COALESCE(ot.item, vmood.item) AS item,
                            COALESCE(item_qty_max_raw_offline, 0) AS item_qty_max_raw_offline, 
                            COALESCE(item_qty_95_raw_offline, 0) AS item_qty_95_raw_offline,
                            COALESCE(ticket_qty_offline, 0) AS ticket_qty_offline,
                            COALESCE(item_qty_max_raw_online, 0) AS item_qty_max_raw_online,
                            COALESCE(item_qty_95_raw_online, 0) AS item_qty_95_raw_online,
                            COALESCE(ticket_qty_online, 0) AS ticket_qty_online
                         FROM (
                            SELECT 
                                item, 
                                max(item_qty) AS item_qty_max_raw_offline, 
                                quantile(0.95)(item_qty)*1.5 AS item_qty_95_raw_offline, 
                                count() AS ticket_qty_offline
                            FROM {database_name}.ods_tickets ot 
                            WHERE store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                            GROUP BY item
                         ) ot
                         FULL OUTER JOIN (
                            SELECT 
                                toInt32OrZero(item) AS item, 
                                max(item_qty) AS item_qty_max_raw_online, 
                                quantile(0.95)(item_qty) AS item_qty_95_raw_online, 
                                count() AS ticket_qty_online
                            FROM {database_name}.view_mart_online_orders_365d vmood 
                            WHERE store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                            GROUP BY item
                            ) vmood 
                        ON ot.item = vmood.item
                        ) """

    # =================================================QUERY EXECUTION=================================================

    logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                               database_name,
                                               work_table_name))

    logger.info("{0}, insert data into {1}.{2}".format(datetime.now(),
                                                       database_name,
                                                       work_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=work_table_name),
                                                BUILD_WORK_SQL.format(database_name=database_name,
                                                                      table_name=work_table_name)])

    logger.info("{0}, END".format(datetime.now()))

    # ==================================================END OPERATOR===================================================


def load_tickets(ds, **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
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
    UNIQUE_STORES_SQL = """
                    SELECT DISTINCT store
                    FROM (
                          SELECT store
                          FROM {database_name}.{tickets_view_name}
                          UNION ALL
                          SELECT store
                          FROM {database_name}.{upsampled_table_name}
                        )
                    ORDER BY store
                """.format(database_name=database_name,
                           tickets_view_name=tickets_view_name,
                           upsampled_table_name=upsampled_table_name)

    #stores = [store[0] for store in run_clickhouse_query(clickhouse_query_list=[UNIQUE_STORES_SQL])]
    stores = [2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 171, 176, 162]

    start_date = date.today() - timedelta(days=load_period)
    #start_date = date(2018, 1, 1)
    end_date = date.today()

    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days + 1)):
            yield start_date + timedelta(n)

    # work_tickets_agg_day_store_item -- агрегация "день, артикул, магазин"
    # toString(item) not like '49%' -- отсекаем "доставку"
    INSERT_WORK_TICKETS_AGG_DAY_STORE_ITEM_SQL = """
                    INSERT INTO {database_name}.{agg_day_store_table_name}
            			SELECT 
            				toDate(sell_date) as sell_date,
            				store,
            				item,
            				count() as tickets_qty,
            				sum(total_cost) as total_cost,
            				sum(item_qty_processed) as item_qty,
            				round(total_cost/sum(item_qty_raw)) as purchase_price
            			FROM {database_name}.{tickets_view_name}
            			WHERE store in ({store_id})
            			  and toString(item) not like '49%'
            			  -- and sell_date >= today() - {load_period}
            			  and sell_date = toDate('{sell_date}')
            			GROUP BY 
            				sell_date, 
            				store, 
            				item,
            				item_qty_raw 
                    """

    # Запрос для отчистки "календаря" от дней по артикулу, которые были до первой продажи артикула
    INSERT_FIRST_SELL_DATE_SQL = """
                        INSERT INTO {database_name}.{first_sell_date_table_name} (item, item_first_sale_date)
                            SELECT 
                                item, 
                                least(sell_date_min_offline, sell_date_min_online, sell_date_min_datablender) as item_first_sale_date
                            FROM (
                                SELECT 
                                    COALESCE(ms_offline.item, COALESCE(ms_online.item, COALESCE(ofsdd.item, 0))) as item, 
                                    multiIf((isNull(sell_date_min_offline)) OR (sell_date_min_offline = toDateTime('1970-01-01 03:00:00')), toDateTime('2040-01-01 00:00:00'), sell_date_min_offline) AS sell_date_min_offline,
                                    multiIf((isNull(sell_date_min_online)) OR (sell_date_min_online = toDateTime('1970-01-01 03:00:00')), toDateTime('2040-01-01 00:00:00'), sell_date_min_online) AS sell_date_min_online,
                                    multiIf((isNull(sell_date_min_datablender)) OR (sell_date_min_datablender = toDateTime('1970-01-01 03:00:00')), toDateTime('2040-01-01 00:00:00'), sell_date_min_datablender) AS sell_date_min_datablender
                                FROM (
                                    SELECT item, min(sell_date) as sell_date_min_offline
                                    FROM {database_name}.{tickets_view_name}
                                    WHERE store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 143, 153, 162, 171, 176)
                                    GROUP BY item
                                ) ms_offline
                                FULL outer join (
                                    SELECT toInt32OrZero(item) as item, min(sell_date) as sell_date_min_online
                                    FROM {database_name}.view_mart_online_orders_365d
                                    WHERE item > 0
                                    GROUP BY item
                                ) ms_online 
                                ON ms_offline.item = ms_online.item
                                FULL outer join (
                                    SELECT item, min(sell_date) as sell_date_min_datablender
                                    FROM {database_name}.ods_first_sale_date_datablender
                                    WHERE store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                                    GROUP BY item
                                ) ofsdd 
                                ON ms_offline.item = ofsdd.item
                            )
                            WHERE item > 0
                        """

    # Построение "календаря по продажам" на каждый день для отношений "магазин-артикул"
    INSERT_WORK_TICKETS_AGG_UPSAMPLED_TMP_SQL = """
                        INSERT INTO {database_name}.{upsampled_tmp_table_name}
                            SELECT 
                                   sell_date, store, item, tickets_qty, total_cost, item_qty, purchase_price
                            FROM (
                                   SELECT 
                                          sell_date, store, item, tickets_qty, total_cost, item_qty, purchase_price
                                   FROM (
                                        -- отношение "артикул-магазин-день" 
                                        SELECT
                                            sell_date, item, tmp, store
                                        FROM (
                                            SELECT DISTINCT
                                                item,
                                                'tmp' as tmp,
                                                store
                                            FROM {database_name}.{agg_day_store_table_name}
                                            WHERE store in ({store_id})
                                                ) items
                                        LEFT JOIN (
                                            SELECT 
                                                'tmp' as tmp,
                                                --Для загрузки начального решения надо раскомментить строку ниже и убрать FROM
                                                toDate('{sell_date}') as sell_date
                                                -- today() - {load_period} + number as sell_date
                                            -- FROM numbers({load_period})
                                        ) dd
                                            ON items.tmp = dd.tmp
                                    ) items
                                LEFT JOIN (
                                -- реальные продажи
                                    SELECT 
                                        toDate(sell_date) as sell_date, item, store, tickets_qty, total_cost, item_qty, purchase_price
                                    FROM {database_name}.{agg_day_store_table_name}
                                    WHERE 
                                    -- Для загрузки начального решения надо раскомментить строку ниже
                                    sell_date = toDate('{sell_date}')
                                    -- sell_date <= today()
                                        and store in ({store_id})
                                ) as tadi
                                ON items.sell_date = tadi.sell_date
                                    and items.item = tadi.item
                                    and items.store = tadi.store
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
                            sell_date, store, tads.item, tickets_qty, total_cost, item_qty, purchase_price
                        FROM (
                                SELECT 
                                    sell_date, store, item, tickets_qty, total_cost, item_qty, 
                                    case 
                                        when 
                                            (purchase_price_origin = 0 or isNaN(purchase_price_origin) or purchase_price_origin is null) 
                                            and purchase_price_past > 0
                                        then purchase_price_past
                                        when 
                                            (purchase_price_origin = 0 or isNaN(purchase_price_origin) or purchase_price_origin is null)
                                            and (purchase_price_past = 0 or isNaN(purchase_price_past) or purchase_price_past is null) 
                                            and purchase_price_future > 0 
                                        then purchase_price_future 
                                        when 
                                            (purchase_price_origin = 0 or isNaN(purchase_price_origin) or purchase_price_origin is null)
                                            and (purchase_price_past = 0 or isNaN(purchase_price_past) or purchase_price_past is null) 
                                            and (purchase_price_future = 0 or isNaN(purchase_price_future) or purchase_price_future is null) 
                                        then -1
                                        else purchase_price_origin
                                    END as purchase_price
                                FROM
                                (
                                    SELECT 
                                        sell_date, store, item, tickets_qty, total_cost, item_qty
                                        , purchase_price as purchase_price_origin
                                        , purchase_price_past
                                    FROM (
                                        SELECT 
                                            sell_date, store, item, tickets_qty, total_cost, item_qty, purchase_price
                                        FROM 
                                            {database_name}.{upsampled_tmp_table_name} wtadsiu
                                        WHERE sell_date = toDate('{sell_date}')
                                            and store in ({store_id})
                                    ) wtadsiu
                                    asof LEFT JOIN (
                                        SELECT 
                                            sell_date, store, item
                                            , purchase_price as purchase_price_past
                                        FROM 
                                            {database_name}.{upsampled_tmp_table_name} wtadsiu
                                        WHERE 
                                            purchase_price_past > 0
                                            and store in ({store_id})
                                            and sell_date < toDate('{sell_date}')
                                    ) as wtadsiu_past 
                                    ON wtadsiu.store = wtadsiu_past.store 
                                        and wtadsiu.item = wtadsiu_past.item 
                                        and wtadsiu.sell_date > wtadsiu_past.sell_date
                                WHERE store in ({store_id})) as wtadsiu
                                asof LEFT JOIN (
                                    SELECT
                                        sell_date
                                        , store
                                        , item
                                        , purchase_price as purchase_price_future
                                    FROM {database_name}.{upsampled_tmp_table_name} wtadsiu
                                    WHERE 
                                        purchase_price_future > 0
                                        and store in ({store_id})
                                        and sell_date > toDate('{sell_date}')
                                ) as wtadsiu_future 
                                    ON wtadsiu.store = wtadsiu_future.store 
                                        and wtadsiu.item = wtadsiu_future.item 
                                        and wtadsiu.sell_date < wtadsiu_future.sell_date
                        ) tads
                        INNER JOIN {database_name}.{first_sell_date_table_name} stat
                            ON toInt32(tads.item) = stat.item 
                        WHERE tads.sell_date >= stat.item_first_sale_date
                        """

    # =================================================QUERY EXECUTION=================================================

    logger.info("{}, start".format(datetime.now()))

    logger.info(
        "{current_datetime}, build {database_name}.{first_sell_date_table_name}".format(current_datetime=datetime.now(),
                                                                                        database_name=database_name,
                                                                                        first_sell_date_table_name=first_sell_date_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=first_sell_date_table_name),
                                                INSERT_FIRST_SELL_DATE_SQL.format(database_name=database_name,
                                                                                  first_sell_date_table_name=first_sell_date_table_name,
                                                                                  tickets_view_name=tickets_view_name)
                                                ])

    for sell_date in daterange(start_date, end_date):
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=agg_day_store_table_name,
                                                                         sell_date=sell_date.strftime("%Y%m%d"))
                                                    ])
        logger.info(
            "{current_datetime}, {database_name}.{agg_day_store_table_name} partition {partition_date} is dropped".format(current_datetime=datetime.now(),
                                                                                                                          database_name=database_name,
                                                                                                                          agg_day_store_table_name=agg_day_store_table_name,
                                                                                                                          partition_date=sell_date))

        for store in stores:
            run_clickhouse_query(logger=logger,
                                 clickhouse_query_list=[INSERT_WORK_TICKETS_AGG_DAY_STORE_ITEM_SQL.format(database_name=database_name,
                                                                                                          agg_day_store_table_name=agg_day_store_table_name,
                                                                                                          tickets_view_name=tickets_view_name,
                                                                                                          store_id=store,
                                                                                                          load_period=load_period,
                                                                                                          sell_date=sell_date)
                                                        ])


    for sell_date in daterange(start_date, end_date):
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=upsampled_tmp_table_name,
                                                                         sell_date=sell_date.strftime("%Y%m%d"))
                                                    ])
        logger.info("{current_datetime}, {database_name}.{upsampled_tmp_table_name} partition {partition_date} is dropped".format(current_datetime=datetime.now(),
                                                                                                                                  database_name=database_name,
                                                                                                                                  upsampled_tmp_table_name=upsampled_tmp_table_name,
                                                                                                                                  partition_date=sell_date))

        for store in stores:

            run_clickhouse_query(logger=logger,
                                 clickhouse_query_list=[INSERT_WORK_TICKETS_AGG_UPSAMPLED_TMP_SQL.format(database_name=database_name,
                                                                                                         upsampled_tmp_table_name=upsampled_tmp_table_name,
                                                                                                         agg_day_store_table_name=agg_day_store_table_name,
                                                                                                         store_id=store,
                                                                                                         load_period=load_period,
                                                                                                         sell_date=sell_date)
                                                        ])


    for sell_date in daterange(start_date, end_date):
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=upsampled_table_name,
                                                                         sell_date=sell_date.strftime("%Y%m%d"))
                                                    ])
        logger.info(
            "{current_datetime}, {database_name}.{upsampled_table_name} partition {partition_date} is dropped".format(current_datetime=datetime.now(),
                                                                                                                      database_name=database_name,
                                                                                                                      upsampled_table_name=upsampled_table_name,
                                                                                                                      partition_date=sell_date))
        logger.info("{current_datetime}, {database_name}.{upsampled_table_name} loading sell_date = {sell_date}".format(current_datetime=datetime.now(),
                                                                                                                        database_name=database_name,
                                                                                                                        upsampled_table_name=upsampled_table_name,
                                                                                                                        sell_date=sell_date))
        for store in stores:
            run_clickhouse_query(logger=logger,
                                 clickhouse_query_list=[INSERT_WORK_TICKETS_AGG_UPSAMPLED_SQL.format(database_name=database_name,
                                                                                                     upsampled_table_name=upsampled_table_name,
                                                                                                     upsampled_tmp_table_name=upsampled_tmp_table_name,
                                                                                                     first_sell_date_table_name=first_sell_date_table_name,
                                                                                                     store_id=store,
                                                                                                     sell_date=sell_date)
                                                        ])

    logger.info("{}, end".format(datetime.now()))

    # ==================================================END OPERATOR===================================================


def load_agg_stock_by_store(database_name,
                            dict_store_table_name,
                            upsampled_table_name,
                            agg_stock_by_store_table_name,
                            week_cnt,
                            stock_table_name,
                            **kwargs):
    
    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Список уникальных магазинов
    GET_STORES_SQL = """
                         SELECT store_id FROM {database_name}.{dict_store_table_name}
                     """.format(database_name=database_name,
                                dict_store_table_name=dict_store_table_name)

    stores = [store_id[0] for store_id in run_clickhouse_query(clickhouse_query_list=[GET_STORES_SQL],
                                                               logger=logger,
                                                               return_result=True)[0]]

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
            FROM {database_name}.{table_name}
            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
            FROM {database_name}.{upsampled_table_name}
            """.format(database_name=database_name,
                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    DROP_PART_SQL = """ALTER TABLE {database_name}.{table_name} DROP PARTITION '{partition_field}'"""

    INSERT_MART_AGG_STOCK_BY_STORE_SQL = """
                                                INSERT INTO {database_name}.{agg_stock_by_store_table_name} 
                                                    (week_num, store, item, avg_week_qty_by_store, max_week_qty_by_store, sum_week_qty, cnt_days_in_week, zero_qty_cnt_by_store, is_zero_qty_week, is_zero_qty_week_part)
                                                SELECT 
                                                        week_num,
                                                        toInt16(store),
                                                        item,
                                                        avg(current_qty_a4s) as avg_week_qty_by_store,
                                                        max(current_qty_a4s) as max_week_qty_by_store,
                                                        sum(current_qty_a4s) as sum_week_qty,
                                                        count(current_qty_a4s) as cnt_days_in_week,
                                                        sum(is_zero_qty) as zero_qty_cnt_by_store,
                                                        if(cnt_days_in_week - zero_qty_cnt_by_store = 0, 1, 0) as is_zero_qty_week,
                                                        if(zero_qty_cnt_by_store <> 0, 1, 0) as is_zero_qty_week_part
                                                    FROM (
                                                        SELECT 
                                                            toRelativeWeekNum(toMonday(toDate(current_snapshot_hour))) AS week_num, 
                                                            id_location AS store, 
                                                            id_item AS item, 
                                                            CASE 
                                                                WHEN current_qty_a4s < 0
                                                                THEN 0
                                                                ELSE current_qty_a4s
                                                            END AS current_qty_a4s,
                                                            if(current_qty_a4s = 0, 1, 0) as is_zero_qty
                                                        FROM {database_name}.{stock_table_name}
                                                        WHERE week_num = {week_num} 
                                                            AND store = '{store_id}'  
                                                    )
                                                    GROUP BY 
                                                        week_num,
                                                        store,
                                                        item
                                                """

    # BUILD agg_stock_by_store_table_name
    agg_stock_by_store_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                          table_name=agg_stock_by_store_table_name,
                                                                                                          week_cnt=week_cnt)],
                                                       return_result=True,
                                                       logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=agg_stock_by_store_table_name,
                                                                                       min_week=agg_stock_by_store_min_week,
                                                                                       max_week=max_week))

    for week_num in range(agg_stock_by_store_min_week, max_week + 1):
        logger.info(DROP_PART_SQL.format(database_name=database_name,
                                         table_name=agg_stock_by_store_table_name,
                                         partition_field=week_num))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=agg_stock_by_store_table_name,
                                                                         partition_field=week_num)])

        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=agg_stock_by_store_table_name))
        for store in stores:
            run_clickhouse_query(logger=logger,
                                 clickhouse_query_list=[
                                     INSERT_MART_AGG_STOCK_BY_STORE_SQL.format(database_name=database_name,
                                                                               agg_stock_by_store_table_name=agg_stock_by_store_table_name,
                                                                               stock_table_name=stock_table_name,
                                                                               week_num=week_num,
                                                                               store_id=store)])

    logger.info(
        "build {database_name}.{table_name} end".format(database_name=database_name,
                                                        table_name=agg_stock_by_store_table_name))


def load_agg_stock_by_week(database_name,
                           agg_stock_by_store_table_name,
                           agg_stock_by_week_table_name,
                           upsampled_table_name,
                           week_cnt,
                           **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
            FROM {database_name}.{table_name}
            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
            FROM {database_name}.{upsampled_table_name}
            """.format(database_name=database_name,
                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    DROP_PART_SQL = """ALTER TABLE {database_name}.{table_name} DROP PARTITION '{partition_field}'"""

    INSERT_MART_AGG_STOCK_BY_WEEK_SQL = """
                                            INSERT INTO {database_name}.{agg_stock_by_week_table_name}  
                                                (week_num, item, avg_qty_by_week, sum_avg_week_qty_by_store, sum_max_week_qty_by_store, num_zero_stock_week_part, num_zero_stock_week_all)
                                            SELECT
                                                src.week_num,
                                                src.item,
                                                agg.avg_qty_by_week,
                                                src.sum_avg_week_qty_by_store,
                                                src.sum_max_week_qty_by_store,
                                                src.num_zero_stock_week_part,
                                                src.num_zero_stock_week_all
                                            FROM (
                                                SELECT
                                                    week_num,
                                                    item,
                                                    sum(avg_week_qty_by_store) as sum_avg_week_qty_by_store,
                                                    sum(max_week_qty_by_store) as sum_max_week_qty_by_store,
                                                    sum(is_zero_qty_week_part) as num_zero_stock_week_part,
                                                    sum(is_zero_qty_week) as num_zero_stock_week_all
                                                FROM {database_name}.{agg_stock_by_store_table_name} 
                                                WHERE week_num = {week_num}
                                                GROUP BY
                                                    week_num,
                                                    item
                                            ) src
                                            INNER JOIN (
                                                SELECT 
                                                    week_num,
                                                    item,
                                                    sum(sum_week_qty)/sum(cnt_days_in_week) AS avg_qty_by_week
                                                FROM {database_name}.{agg_stock_by_store_table_name} 
                                                WHERE week_num = {week_num}
                                                GROUP BY
                                                    week_num,
                                                    item
                                            ) agg
                                            ON src.week_num = agg.week_num 
                                                AND src.item = agg.item
                                            """

    # BUILD agg_stock_by_week_table_name
    agg_stock_by_store_min_week = \
    run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                            table_name=agg_stock_by_store_table_name,
                                                                            week_cnt=week_cnt)],
                         return_result=True,
                         logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=agg_stock_by_store_table_name,
                                                                                       min_week=agg_stock_by_store_min_week,
                                                                                       max_week=max_week))

    for week_num in range(agg_stock_by_store_min_week, max_week + 1):

        logger.info(DROP_PART_SQL.format(database_name=database_name,
                                         table_name=agg_stock_by_week_table_name,
                                         partition_field=week_num))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=agg_stock_by_week_table_name,
                                                                         partition_field=week_num)])
        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=agg_stock_by_week_table_name))

        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[
                                 INSERT_MART_AGG_STOCK_BY_WEEK_SQL.format(database_name=database_name,
                                                                          agg_stock_by_week_table_name=agg_stock_by_week_table_name,
                                                                          agg_stock_by_store_table_name=agg_stock_by_store_table_name,
                                                                          week_num=week_num)])

        logger.info(
            "build {database_name}.{table_name} end".format(database_name=database_name,
                                                            table_name=agg_stock_by_store_table_name))


def load_item_attr(database_name,
                   item_attr_table_name,
                   subclass_table_name,
                   item_master_table_name,
                   products_info_table_name,
                   **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_ITEM_ATTR_SQL = """
                                INSERT INTO {database_name}.{item_attr_table_name}
    								(item, dept_class_subclass, model)
    							SELECT 
                                    im.item as item,
                                    toString(im.dept) || '|' || toString(im.class) || '|' || toString(im.subclass) as dept_class_subclass,
                                    toString(coalesce(opi.model, -1)) as model
                                FROM {database_name}.{item_master_table_name} im
                                INNER JOIN (
    	                            SELECT toString(item) as item, model
    	                            FROM {database_name}.{products_info_table_name} 
    	                            WHERE is_actual = '1'
                                ) opi
                                ON opi.item = im.item
                                """

    # BUILD item_attr_table_name
    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=item_attr_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=item_attr_table_name)])

    logger.info(
        "inserting data into {database_name}.{table_name}".format(database_name=database_name,
                                                                  table_name=item_attr_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[INSERT_MART_ITEM_ATTR_SQL.format(database_name=database_name,
                                                                                 item_attr_table_name=item_attr_table_name,
                                                                                 subclass_table_name=subclass_table_name,
                                                                                 item_master_table_name=item_master_table_name,
                                                                                 products_info_table_name=products_info_table_name)])

    logger.info(
        "build {database_name}.{table_name} end".format(database_name=database_name,
                                                        table_name=item_attr_table_name))


def load_agg_tickets_by_store(database_name,
                              item_attr_table_name,
                              agg_tickets_by_store_table_name,
                              week_cnt,
                              upsampled_table_name,
                              dict_store_table_name,
                              uda_item_date_view_name,
                              **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                    SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                    FROM {database_name}.{table_name}
                    """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                    SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                    FROM {database_name}.{upsampled_table_name}
                    """.format(database_name=database_name,
                               upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    # Список уникальных магазинов
    GET_STORES_SQL = """
                         SELECT store_id FROM {database_name}.{dict_store_table_name}
                     """.format(database_name=database_name,
                                dict_store_table_name=dict_store_table_name)

    stores = [store_id[0] for store_id in run_clickhouse_query(clickhouse_query_list=[GET_STORES_SQL],
                                                               logger=logger,
                                                               return_result=True)[0]]

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_AGG_TICKETS_BY_STORE_SQL = """
                                            INSERT INTO {database_name}.{agg_tickets_by_store_table_name}
                                                (year, week, week_num, store, item, dept_class_subclass, model, week_item_qty, sum_purchase_price, count_tickets)
                                            SELECT ups."year"
                                                   , ups.week
                                                   , ups.week_num
                                                   , ups.store
                                                   , ups.item
                                                   , attr.dept_class_subclass
                                                   , attr.model
                                                   , ups.week_item_qty
                                                   , ups.sum_purchase_price
                                                   , ups.count_tickets
                                            FROM 
                                                (
                                                    SELECT
                                                           toYear(toMonday(tads.sell_date)) as "year"
                                                         , toRelativeWeekNum(toMonday(tads.sell_date)) as week_num
                                                         , toRelativeWeekNum(toMonday(tads.sell_date)) - toRelativeWeekNum(toDateTime(toString(toYear(toMonday(tads.sell_date))-1) || '-12-31 00:00:00')) as week
                                                         , tads.store
                                                         , toUInt32(tads.item) as item 
                                                         , sum(item_qty) as week_item_qty
                                                         , sum(purchase_price) as sum_purchase_price
                                                         , count(item_qty) as count_tickets
                                                    FROM {database_name}.{upsampled_table_name} tads
                                                    LEFT JOIN (
                                                        SELECT item, uda_date 
                                                        FROM {database_name}.{uda_item_date_view_name} 
                                                        WHERE uda_id = 6
                                                    ) avs
                                                    ON tads.item = avs.item
                                                    WHERE (avs.uda_date is null OR avs.uda_date > addDays(sell_date, 14)) 
                                                        AND week_num = {week_num}
                                                        AND tads.store = {store_id}
                                                    GROUP BY toYear(toMonday(tads.sell_date)) as "year"
                                                           , toRelativeWeekNum(toMonday(tads.sell_date)) - toRelativeWeekNum(toDateTime(toString(toYear(toMonday(tads.sell_date))-1) || '-12-31 00:00:00')) as week
                                                           , toRelativeWeekNum(toMonday(tads.sell_date)) as week_num
                                                           , tads.store
                                                           , tads.item
                                                ) ups
                                            LEFT JOIN 
                                            	{database_name}.{item_attr_table_name} attr
                                            ON ups.item = attr.item
                                            """

    # BUILD agg_tickets_by_store_table_name
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=agg_tickets_by_store_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=agg_tickets_by_store_table_name)])

    agg_tickets_by_store_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                            table_name=agg_tickets_by_store_table_name,
                                                                                                            week_cnt=week_cnt)],
                                                         return_result=True,
                                                         logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=agg_tickets_by_store_table_name,
                                                                                       min_week=agg_tickets_by_store_min_week,
                                                                                       max_week=max_week))

    for week_num in range(agg_tickets_by_store_min_week, max_week + 1):

        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=agg_tickets_by_store_table_name))
        for store in stores:
            run_clickhouse_query(logger=logger,
                                 clickhouse_query_list=[
                                     INSERT_MART_AGG_TICKETS_BY_STORE_SQL.format(database_name=database_name,
                                                                                 agg_tickets_by_store_table_name=agg_tickets_by_store_table_name,
                                                                                 upsampled_table_name=upsampled_table_name,
                                                                                 item_attr_table_name=item_attr_table_name,
                                                                                 uda_item_date_view_name=uda_item_date_view_name,
                                                                                 week_num=week_num,
                                                                                 store_id=store)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=agg_tickets_by_store_table_name))


def load_avg_tickets_by_week(database_name,
                             avg_tickets_by_week_table_name,
                             agg_tickets_by_store_table_name,
                             week_cnt,
                             upsampled_table_name,
                             agg_stock_by_store_table_name,
                             **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                        SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                        FROM {database_name}.{table_name}
                        """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                        SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                        FROM {database_name}.{upsampled_table_name}
                        """.format(database_name=database_name,
                                   upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_AVG_TICKETS_BY_WEEK_SQL = """
                                            INSERT INTO {database_name}.{avg_tickets_by_week_table_name}
                                                (year, week, week_num, item, dept_class_subclass, model, avg_item_qty_scaled_with_stock)
                                            SELECT 
                                                year,
                                                week,
                                                week_num,
                                                item,
                                                dept_class_subclass,
                                                model,
                                                if(avg(item_qty_scaled_with_stock) is NULL, NULL, avg(item_qty_scaled_with_stock)) AS avg_item_qty_scaled_with_stock
                                            FROM (
                                                SELECT         
                                                    scld.year,
                                                    scld.week,
                                                    scld.week_num,
                                                    scld.item,
                                                    scld.dept_class_subclass,
                                                    scld.model,
                                                    scld.store,
                                                    CASE
                                                        WHEN stck.item = 0 or isNaN(stck.item) or stck.item is null
                                                        THEN NULL
                                                        ELSE scld.item_qty_scaled
                                                    END AS item_qty_scaled_with_stock
                                                FROM (
                                                    SELECT 
                                                        src.year,
                                                        src.week,
                                                        src.week_num,
                                                        src.store,
                                                        src.item,
                                                        src.dept_class_subclass,
                                                        src.model,
                                                        CASE 
                                                            WHEN agg.max_item_qty = 0 or isNaN(agg.max_item_qty) or agg.max_item_qty is null 
                                                            THEN 0
                                                            ELSE src.week_item_qty/agg.max_item_qty
                                                        END AS item_qty_scaled
                                                    FROM {database_name}.{agg_tickets_by_store_table_name} src
                                                    INNER JOIN (
                                                        SELECT 
                                                            item, 
                                                            store, 
                                                            max(week_item_qty) as max_item_qty 
                                                        FROM {database_name}.{agg_tickets_by_store_table_name} 
                                                        GROUP BY item, store
                                                    ) agg
                                                    ON src.item = agg.item
                                                        AND src.store = agg.store
                                                    WHERE src.week_num = {week_num}
                                                ) scld
                                                LEFT JOIN (
                                                    SELECT week_num, item, store  
                                                    FROM {database_name}.{agg_stock_by_store_table_name}
                                                    WHERE week_num = {week_num}
                                                ) stck
                                                ON toUInt32(scld.week_num) = stck.week_num
                                                    AND scld.item = stck.item
                                                    AND toString(scld.store) = stck.store
                                            )
                                            GROUP BY 
                                                year,
                                                week,
                                                week_num,
                                                item,
                                                dept_class_subclass,
                                                model
                                        """

    # BUILD avg_tickets_by_week_table_name
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=avg_tickets_by_week_table_name)])

    avg_tickets_by_week_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                           table_name=avg_tickets_by_week_table_name,
                                                                                                           week_cnt=week_cnt)],
                                                        return_result=True,
                                                        logger=logger)[0][0][0]

    logger.info("build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                               table_name=avg_tickets_by_week_table_name,
                                                                                               min_week=avg_tickets_by_week_min_week,
                                                                                               max_week=max_week))

    for week_num in range(avg_tickets_by_week_min_week, max_week + 1):

        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=avg_tickets_by_week_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[INSERT_MART_AVG_TICKETS_BY_WEEK_SQL.format(database_name=database_name,
                                                                                               avg_tickets_by_week_table_name=avg_tickets_by_week_table_name,
                                                                                               agg_tickets_by_store_table_name=agg_tickets_by_store_table_name,
                                                                                               agg_stock_by_store_table_name = agg_stock_by_store_table_name,
                                                                                               week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=avg_tickets_by_week_table_name))


def load_sum_tickets_by_week(database_name,
                             sum_tickets_by_week_table_name,
                             agg_tickets_by_store_table_name,
                             week_cnt,
                             upsampled_table_name,
                             **kwargs):

    from src.sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                        SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                        FROM {database_name}.{table_name}
                        """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                        SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                        FROM {database_name}.{upsampled_table_name}
                        """.format(database_name=database_name,
                                   upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_SUM_TICKETS_BY_WEEK_SQL = """
                                            INSERT INTO {database_name}.{sum_tickets_by_week_table_name}
                                                (year, week, week_num, item, dept_class_subclass, model, sum_item_qty, avg_purchase_price)
                                            SELECT
                                                year,
                                                week,
                                                week_num,
                                                item,
                                                dept_class_subclass,
                                                model,
                                                sum_item_qty,
                                                sum_purchase_price_by_week/count_tickets_by_week AS avg_purchase_price
                                            FROM (
                                                SELECT 
                                                    year,
                                                    week,
                                                    week_num,
                                                    item,
                                                    dept_class_subclass,
                                                    model,
                                                    sum(week_item_qty) AS sum_item_qty,
                                                    sum(sum_purchase_price) as sum_purchase_price_by_week,
                                                    sum(count_tickets) as count_tickets_by_week
                                                FROM {database_name}.{agg_tickets_by_store_table_name}
                                                    WHERE week_num = {week_num}
                                                GROUP BY 
                                                    year,
                                                    week,
                                                    week_num,
                                                    item,
                                                    dept_class_subclass,
                                                    model
                                                )
                                            """



    # BUILD sum_tickets_by_week_table_name
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=sum_tickets_by_week_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=sum_tickets_by_week_table_name)])

    sum_tickets_by_week_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                           table_name=sum_tickets_by_week_table_name,
                                                                                                           week_cnt=week_cnt)],
                                                        return_result=True,
                                                        logger=logger)[0][0][0]

    for week_num in range(sum_tickets_by_week_min_week, max_week + 1):

        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(
            week_num=week_num,
            database_name=database_name,
            table_name=sum_tickets_by_week_table_name))

        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[INSERT_MART_SUM_TICKETS_BY_WEEK_SQL.format(database_name=database_name,
                                                                                               sum_tickets_by_week_table_name=sum_tickets_by_week_table_name,
                                                                                               agg_tickets_by_store_table_name=agg_tickets_by_store_table_name,
                                                                                               week_num=week_num)])



    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=sum_tickets_by_week_table_name))


def load_avg_week_tickets_by_class(database_name,
                                   avg_week_tickets_by_class_table_name,
                                   avg_tickets_by_week_view_name,
                                   **kwargs):
    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_AVG_WEEK_TICKETS_BY_CLASS_SQL = """
                                                 INSERT INTO {database_name}.{avg_week_tickets_by_class_table_name}
                                                     (year, week, week_num, dept_class, avg_item_qty_by_class)
                                                 SELECT 
                                                     src.year,
                                                     src.week,
                                                     src.week_num,
                                                     arrayStringConcat(arraySlice(splitByChar('|', src.dept_class_subclass), 1, 2), '|') as dept_class,
                                                     avg(src.avg_item_qty_scaled_with_stock) as avg_item_qty_by_class
                                                 FROM 
                                                     {database_name}.{avg_tickets_by_week_view_name} src
                                                 GROUP BY 
                                                     src.year,
                                                     src.week,
                                                     src.week_num, 
                                                     dept_class
                                                 """

    # BUILD avg_week_tickets_by_class_table_name
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=avg_week_tickets_by_class_table_name))
    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=avg_week_tickets_by_class_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=avg_week_tickets_by_class_table_name)])

    logger.info(
        "inserting data into {database_name}.{table_name}".format(database_name=database_name,
                                                                  table_name=avg_week_tickets_by_class_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[INSERT_MART_AVG_WEEK_TICKETS_BY_CLASS_SQL.format(
                             database_name=database_name,
                             avg_week_tickets_by_class_table_name=avg_week_tickets_by_class_table_name,
                             avg_tickets_by_week_view_name=avg_tickets_by_week_view_name)])

    logger.info(
        "build {database_name}.{table_name} end".format(database_name=database_name,
                                                        table_name=avg_week_tickets_by_class_table_name))


def load_avg_week_tickets_by_subclass(database_name,
                                      avg_week_tickets_by_subclass_table_name,
                                      avg_tickets_by_week_view_name,
                                      **kwargs):
    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_AVG_WEEK_TICKETS_BY_SUBCLASS_SQL = """
                                                    INSERT INTO {database_name}.{avg_week_tickets_by_subclass_table_name}
                                                        (year, week, week_num, dept_class_subclass, avg_item_qty_by_subclass)
                                                    SELECT 
                                                        src.year,
                                                        src.week,
                                                        src.week_num,
                                                        src.dept_class_subclass,
                                                        avg(src.avg_item_qty_scaled_with_stock) as avg_item_qty_by_class
                                                    FROM 
                                                        {database_name}.{avg_tickets_by_week_view_name} src
                                                    GROUP BY 
                                                        src.year,
                                                        src.week,
                                                        src.week_num, 
                                                        dept_class_subclass
                                                    """

    # BUILD avg_week_tickets_by_subclass_table_name
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=avg_week_tickets_by_subclass_table_name))
    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=avg_week_tickets_by_subclass_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=avg_week_tickets_by_subclass_table_name)])

    logger.info(
        "inserting data into {database_name}.{table_name}".format(database_name=database_name,
                                                                  table_name=avg_week_tickets_by_subclass_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[INSERT_MART_AVG_WEEK_TICKETS_BY_SUBCLASS_SQL.format(
                             database_name=database_name,
                             avg_week_tickets_by_subclass_table_name=avg_week_tickets_by_subclass_table_name,
                             avg_tickets_by_week_view_name=avg_tickets_by_week_view_name)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=avg_week_tickets_by_subclass_table_name))


def load_avg_week_tickets_by_model(database_name,
                                   avg_week_tickets_by_model_table_name,
                                   avg_tickets_by_week_view_name,
                                   **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_AVG_WEEK_TICKETS_BY_MODEL_SQL = """
                                                INSERT INTO {database_name}.{avg_week_tickets_by_model_table_name}
                                                    (year, week, week_num, model, avg_item_qty_by_model)
                                                SELECT 
                                                    src.year,
                                                    src.week,
                                                    src.week_num,
                                                    src.model,
                                                    avg(src.avg_item_qty_scaled_with_stock) as avg_item_qty_by_model
                                                FROM 
                                                    {database_name}.{avg_tickets_by_week_view_name} src
                                                GROUP BY 
                                                    src.year,
                                                    src.week,
                                                    src.week_num, 
                                                    src.model
                                                """

    # BUILD avg_week_tickets_by_model_table_name
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=avg_week_tickets_by_model_table_name))
    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=avg_week_tickets_by_model_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=avg_week_tickets_by_model_table_name)])

    logger.info(
        "inserting data into {database_name}.{table_name}".format(database_name=database_name,
                                                                  table_name=avg_week_tickets_by_model_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[INSERT_MART_AVG_WEEK_TICKETS_BY_MODEL_SQL.format(
                             database_name=database_name,
                             avg_week_tickets_by_model_table_name=avg_week_tickets_by_model_table_name,
                             avg_tickets_by_week_view_name=avg_tickets_by_week_view_name)])

    logger.info(
        "build {database_name}.{avg_week_tickets_by_model_table_name} end".format(database_name=database_name,
                                                                                  avg_week_tickets_by_model_table_name=avg_week_tickets_by_model_table_name))


def load_agg_tickets_attr_by_week(database_name,
                                  agg_tickets_attr_by_week_table_name,
                                  sum_tickets_by_week_table_name,
                                  avg_week_tickets_by_class_view_name,
                                  avg_week_tickets_by_subclass_view_name,
                                  avg_week_tickets_by_model_view_name,
                                  upsampled_table_name,
                                  week_cnt,
                                  agg_tickets_attr_by_week_max_val_table_name,
                                  **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                            FROM {database_name}.{table_name}
                            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                            FROM {database_name}.{upsampled_table_name}
                            """.format(database_name=database_name,
                                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_AGG_TICKETS_ATTR_BY_WEEK_SQL = """
                                                INSERT INTO {database_name}.{agg_tickets_attr_by_week_table_name}
                                                    (year, week, week_num, item, dept_class_subclass, model, sum_item_qty, avg_purchase_price, avg_item_qty_by_class_scaled, avg_item_qty_by_subclass_scaled, avg_item_qty_by_model_scaled)
                                                SELECT 
                                                    src.year,
                                                    src.week,
                                                    src.week_num,
                                                    src.item,
                                                    src.dept_class_subclass,
                                                    src.model,
                                                    src.sum_item_qty,
                                                    src.avg_purchase_price,
                                                    c.avg_item_qty_by_class_scaled,
                                                    s.avg_item_qty_by_subclass_scaled,
                                                    m.avg_item_qty_by_model_scaled
                                                FROM {database_name}.{sum_tickets_by_week_table_name} src
                                                LEFT JOIN (
                                                    SELECT 
                                                        week_num,
                                                        dept_class,
                                                        avg_item_qty_by_class_scaled
                                                    FROM {database_name}.{avg_week_tickets_by_class_view_name}
                                                    WHERE week_num = {week_num}
                                                ) c
                                                ON arrayStringConcat(arraySlice(splitByChar('|', src.dept_class_subclass), 1, 2), '|') = c.dept_class 
                                                    AND src.week_num = c.week_num
                                                LEFT JOIN (
                                                    SELECT 
                                                        week_num,
                                                        dept_class_subclass,
                                                        avg_item_qty_by_subclass_scaled
                                                    FROM {database_name}.{avg_week_tickets_by_subclass_view_name}
                                                    WHERE week_num = {week_num}
                                                ) s
                                                ON src.dept_class_subclass = s.dept_class_subclass 
                                                    AND src.week_num = s.week_num
                                                LEFT JOIN (
                                                    SELECT 
                                                        week_num,
                                                        model,
                                                        avg_item_qty_by_model_scaled
                                                    FROM {database_name}.{avg_week_tickets_by_model_view_name}
                                                    WHERE week_num = {week_num}
                                                ) m
                                                ON src.model = m.model 
                                                    AND src.week_num = m.week_num
                                                WHERE src.week_num = {week_num}
                                            """

    INSERT_WORK_AGG_TICKETS_ATTR_BY_WEEK_MAX_VAL_SQL = """
                                                        INSERT INTO {database_name}.{agg_tickets_attr_by_week_max_val_table_name}
                                                            (item, max_sum_item_qty)
                                                        SELECT 
                                                            item,
                                                            max(sum_item_qty) as max_sum_item_qty
                                                        FROM {database_name}.{agg_tickets_attr_by_week_table_name}
                                                        GROUP BY item
                                                    """


    # BUILD agg_tickets_attr_by_week
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=agg_tickets_attr_by_week_table_name))
    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=agg_tickets_attr_by_week_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=agg_tickets_attr_by_week_table_name)])

    agg_tickets_attr_by_week_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                                table_name=agg_tickets_attr_by_week_table_name,
                                                                                                                week_cnt=week_cnt)],
                                                             return_result=True,
                                                             logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=agg_tickets_attr_by_week_table_name,
                                                                                       min_week=agg_tickets_attr_by_week_min_week,
                                                                                       max_week=max_week))

    for week_num in range(agg_tickets_attr_by_week_min_week, max_week + 1):
        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=agg_tickets_attr_by_week_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[
                                 INSERT_MART_AGG_TICKETS_ATTR_BY_WEEK_SQL.format(database_name=database_name,
                                                                                 agg_tickets_attr_by_week_table_name=agg_tickets_attr_by_week_table_name,
                                                                                 sum_tickets_by_week_table_name=sum_tickets_by_week_table_name,
                                                                                 avg_week_tickets_by_class_view_name=avg_week_tickets_by_class_view_name,
                                                                                 avg_week_tickets_by_subclass_view_name=avg_week_tickets_by_subclass_view_name,
                                                                                 avg_week_tickets_by_model_view_name=avg_week_tickets_by_model_view_name,
                                                                                 week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=agg_tickets_attr_by_week_table_name))

    # BUILD agg_tickets_attr_by_week_max_val
    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=agg_tickets_attr_by_week_max_val_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=agg_tickets_attr_by_week_max_val_table_name)])

    logger.info(
        "inserting data into {database_name}.{table_name}".format(database_name=database_name,
                                                                  table_name=agg_tickets_attr_by_week_max_val_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[
                             INSERT_WORK_AGG_TICKETS_ATTR_BY_WEEK_MAX_VAL_SQL.format(database_name=database_name,
                                                                                     agg_tickets_attr_by_week_max_val_table_name=agg_tickets_attr_by_week_max_val_table_name,
                                                                                     agg_tickets_attr_by_week_table_name=agg_tickets_attr_by_week_table_name)])

    logger.info(
        "build {database_name}.{avg_week_tickets_by_model_table_name} end".format(database_name=database_name,
                                                                                  avg_week_tickets_by_model_table_name=agg_tickets_attr_by_week_max_val_table_name))


def load_ticket_features_by_week(database_name,
                                 ticket_features_by_week_table_name,
                                 agg_tickets_attr_by_week_view_name,
                                 current_year_week_features_left,
                                 current_year_week_features_right,
                                 upsampled_table_name,
                                 week_cnt,
                                 **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                            FROM {database_name}.{table_name}
                            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                            FROM {database_name}.{upsampled_table_name}
                            """.format(database_name=database_name,
                                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    DROP_TABLE_SQL = """DROP TABLE IF EXISTS {database_name}.{table_name}"""

    # GENERATE QUERIES FOR MART_TICKET_FEATURES_BY_WEEK

    DDL_MART_TICKET_FEATURES_BY_WEEK_SQL = """ 
                                                CREATE TABLE IF NOT EXISTS {database_name}.{ticket_features_by_week_table_name}
                                                (
                                                    `year` UInt32,
                                                    `week` UInt32,
                                                    `week_num` UInt16,
                                                    `item` UInt32,
                                                    `sum_item_qty_scaled` Nullable(Float32),
                                                    `avg_purchase_price` Nullable(Float32),
                                                    `avg_item_qty_by_class_scaled` Nullable(Float32),
                                                    `avg_item_qty_by_subclass_scaled` Nullable(Float32),
                                                    `avg_item_qty_by_model_scaled` Nullable(Float32)"""

    INSERT_MART_TICKET_FEATURES_BY_WEEK_STATEMENT = """
                                                    INSERT INTO {database_name}.{ticket_features_by_week_table_name} 
                                                        (year, week, week_num, item, sum_item_qty_scaled, avg_purchase_price, avg_item_qty_by_class_scaled, avg_item_qty_by_subclass_scaled, avg_item_qty_by_model_scaled"""

    INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL = """
                                                    SELECT
                                                        src.year,
                                                        src.week,
                                                        src.week_num,
                                                        src.item,
                                                        src.sum_item_qty_scaled,
                                                        src.avg_purchase_price,
                                                        src.avg_item_qty_by_class_scaled,
                                                        src.avg_item_qty_by_subclass_scaled,
                                                        src.avg_item_qty_by_model_scaled"""

    for i in range(1, current_year_week_features_left + 1):
        INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL = INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL + """
                                                        , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.sum_item_qty_scaled, NULL)) as sum_item_qty_scaled_prev_w{i}
                                                        , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.avg_purchase_price, NULL)) as avg_purchase_price_prev_w{i}
                                                        , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_prev_w{i}
                                                        , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_prev_w{i}
                                                        , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_prev_w{i}""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_WEEK_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_WEEK_STATEMENT + ", sum_item_qty_scaled_prev_w{i}, avg_purchase_price_prev_w{i}, avg_item_qty_by_class_prev_w{i}, avg_item_qty_by_subclass_prev_w{i}, avg_item_qty_by_model_prev_w{i}".format(
            i=i)
        DDL_MART_TICKET_FEATURES_BY_WEEK_SQL = DDL_MART_TICKET_FEATURES_BY_WEEK_SQL + """
                                                    , `sum_item_qty_scaled_prev_w{i}` Nullable(Float32)
                                                    , `avg_purchase_price_prev_w{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_class_prev_w{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_subclass_prev_w{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_model_prev_w{i}` Nullable(Float32)""".format(i=i)

    for i in range(1, current_year_week_features_right + 1):
        INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL = INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL + """
                                                        , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.sum_item_qty_scaled, NULL)) as sum_item_qty_scaled_next_w{i}
                                                        , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.avg_purchase_price, NULL)) as avg_purchase_price_next_w{i}
                                                        , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_next_w{i}
                                                        , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_next_w{i}
                                                        , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_next_w{i}""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_WEEK_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_WEEK_STATEMENT + ", sum_item_qty_scaled_next_w{i}, avg_purchase_price_next_w{i}, avg_item_qty_by_class_next_w{i}, avg_item_qty_by_subclass_next_w{i}, avg_item_qty_by_model_next_w{i}".format(
            i=i)
        DDL_MART_TICKET_FEATURES_BY_WEEK_SQL = DDL_MART_TICKET_FEATURES_BY_WEEK_SQL + """
                                                    , `sum_item_qty_scaled_next_w{i}` Nullable(Float32)
                                                    , `avg_purchase_price_next_w{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_class_next_w{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_subclass_next_w{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_model_next_w{i}` Nullable(Float32)""".format(i=i)

    DDL_MART_TICKET_FEATURES_BY_WEEK_SQL = DDL_MART_TICKET_FEATURES_BY_WEEK_SQL + """
                                                )
                                                ENGINE = MergeTree()
                                                PARTITION BY week_num
                                                ORDER BY (week_num,
                                                 item)
                                                SETTINGS index_granularity = 8192;
                                                """
    INSERT_MART_TICKET_FEATURES_BY_WEEK_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_WEEK_STATEMENT + ")"
    INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL = INSERT_MART_TICKET_FEATURES_BY_WEEK_STATEMENT + INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL + """
                                                    FROM (
                                                        SELECT
                                                            year,
                                                            week,
                                                            week_num,
                                                            item,
                                                            sum_item_qty_scaled,
                                                            avg_purchase_price,
                                                            avg_item_qty_by_class_scaled,
                                                            avg_item_qty_by_subclass_scaled,
                                                            avg_item_qty_by_model_scaled
                                                        FROM {database_name}.{agg_tickets_attr_by_week_view_name}
                                                        WHERE week_num = {week_num}
                                                        ) src
                                                    LEFT JOIN (
                                                        SELECT 
                                                            year,
                                                            week,
                                                            week_num,
                                                            item,
                                                            sum_item_qty_scaled,
                                                            avg_purchase_price,
                                                            avg_item_qty_by_class_scaled,
                                                            avg_item_qty_by_subclass_scaled,
                                                            avg_item_qty_by_model_scaled
                                                        FROM {database_name}.{agg_tickets_attr_by_week_view_name} 
                                                        WHERE week_num between ({week_num} - {current_year_week_features_left}) AND ({week_num} + {current_year_week_features_right})
                                                        ) prev
                                                    ON src.item = prev.item
                                                    GROUP BY 
                                                        src.year,
                                                        src.week,
                                                        src.week_num,
                                                        src.item,
                                                        src.sum_item_qty_scaled,
                                                        src.avg_purchase_price,
                                                        src.avg_item_qty_by_class_scaled,
                                                        src.avg_item_qty_by_subclass_scaled,
                                                        src.avg_item_qty_by_model_scaled
                                                    """

    # BUILD ticket_features_by_week
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=ticket_features_by_week_table_name))
    logger.info(DROP_TABLE_SQL.format(database_name=database_name,
                                      table_name=ticket_features_by_week_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DROP_TABLE_SQL.format(database_name=database_name,
                                                                      table_name=ticket_features_by_week_table_name)])

    logger.info(DDL_MART_TICKET_FEATURES_BY_WEEK_SQL.format(database_name=database_name,
                                                            ticket_features_by_week_table_name=ticket_features_by_week_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DDL_MART_TICKET_FEATURES_BY_WEEK_SQL.format(database_name=database_name,
                                                                                            ticket_features_by_week_table_name=ticket_features_by_week_table_name)])

    ticket_features_by_week_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                               table_name=ticket_features_by_week_table_name,
                                                                                                               week_cnt=week_cnt)],
                                                            return_result=True,
                                                            logger=logger)[0][0][0]

    logger.info("build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                               table_name=ticket_features_by_week_table_name,
                                                                                               min_week=ticket_features_by_week_min_week,
                                                                                               max_week=max_week))

    logger.info(
        INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL.format(database_name=database_name,
                                                       ticket_features_by_week_table_name=ticket_features_by_week_table_name,
                                                       agg_tickets_attr_by_week_view_name=agg_tickets_attr_by_week_view_name,
                                                       current_year_week_features_left=current_year_week_features_left,
                                                       current_year_week_features_right=current_year_week_features_right,
                                                       week_num='0'))

    for week_num in range(ticket_features_by_week_min_week, max_week + 1):

        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=ticket_features_by_week_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[INSERT_MART_TICKET_FEATURES_BY_WEEK_SQL.format(database_name=database_name,
                                                                                                   ticket_features_by_week_table_name=ticket_features_by_week_table_name,
                                                                                                   agg_tickets_attr_by_week_view_name=agg_tickets_attr_by_week_view_name,
                                                                                                   current_year_week_features_left=current_year_week_features_left,
                                                                                                   current_year_week_features_right=current_year_week_features_right,
                                                                                                   week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=ticket_features_by_week_table_name))


def load_ticket_features_by_year(database_name,
                                 ticket_features_by_year_table_name,
                                 agg_tickets_attr_by_week_view_name,
                                 calendar_table_name,
                                 last_year_week_features_left,
                                 last_year_week_features_right,
                                 upsampled_table_name,
                                 week_cnt,
                                 **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                            FROM {database_name}.{table_name}
                            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                            FROM {database_name}.{upsampled_table_name}
                            """.format(database_name=database_name,
                                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    DROP_TABLE_SQL = """DROP TABLE IF EXISTS {database_name}.{table_name}"""

    # GENERATE QUERIES FOR MART_TICKET_FEATURES_BY_YEAR

    DDL_MART_TICKET_FEATURES_BY_YEAR_SQL = """ 
                                                CREATE TABLE IF NOT EXISTS {database_name}.{ticket_features_by_year_table_name}
                                                (
                                                    `year` UInt32,
                                                    `week` UInt32,
                                                    `week_num` UInt16,
                                                    `item` UInt32,
                                                    `sum_item_qty_scaled_y` Nullable(Float32),
                                                    `avg_purchase_price_y` Nullable(Float32),
                                                    `avg_item_qty_by_class_scaled_y` Nullable(Float32),
                                                    `avg_item_qty_by_subclass_scaled_y` Nullable(Float32),
                                                    `avg_item_qty_by_model_scaled_y` Nullable(Float32)"""

    INSERT_MART_TICKET_FEATURES_BY_YEAR_STATEMENT = """
                                                    INSERT INTO {database_name}.{ticket_features_by_year_table_name} 
                                                        (year, week, week_num, item, sum_item_qty_scaled_y, avg_purchase_price_y, avg_item_qty_by_class_scaled_y, avg_item_qty_by_subclass_scaled_y, avg_item_qty_by_model_scaled_y"""

    INSERT_MART_TICKET_FEATURES_BY_YEAR_SQL = """
                                                    SELECT
                                                        src.year,
                                                        src.week,
                                                        src.week_num,
                                                        src.item,
                                                        sum(if(prev.week_num = wcft.prev_week_num, prev.sum_item_qty_scaled, NULL)) as sum_item_qty_scaled_y,
                                                        sum(if(prev.week_num = wcft.prev_week_num, prev.avg_purchase_price, NULL)) as avg_purchase_price_y,
                                                        sum(if(prev.week_num = wcft.prev_week_num, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_scaled_y,
                                                        sum(if(prev.week_num = wcft.prev_week_num, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_scaled_y,
                                                        sum(if(prev.week_num = wcft.prev_week_num, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_scaled_y"""
    for i in range(1, last_year_week_features_left + 1):
        INSERT_MART_TICKET_FEATURES_BY_YEAR_SQL = INSERT_MART_TICKET_FEATURES_BY_YEAR_SQL + """
                                                        , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.sum_item_qty_scaled, NULL)) as sum_item_qty_scaled_yw{i}
                                                        , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.avg_purchase_price, NULL)) as avg_purchase_price_yw{i}
                                                        , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_scaled_yw{i}
                                                        , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_scaled_yw{i}
                                                        , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_scaled_yw{i}""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_YEAR_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_YEAR_STATEMENT + ", sum_item_qty_scaled_yw{i}, avg_purchase_price_yw{i}, avg_item_qty_by_class_scaled_yw{i}, avg_item_qty_by_subclass_scaled_yw{i}, avg_item_qty_by_model_scaled_yw{i}".format(
            i=i)
        DDL_MART_TICKET_FEATURES_BY_YEAR_SQL = DDL_MART_TICKET_FEATURES_BY_YEAR_SQL + """
                                                    , `sum_item_qty_scaled_yw{i}` Nullable(Float32)
                                                    , `avg_purchase_price_yw{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_class_scaled_yw{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_subclass_scaled_yw{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_model_scaled_yw{i}` Nullable(Float32)""".format(
            i=i)

    for i in range(1, last_year_week_features_right + 1):
        INSERT_MART_TICKET_FEATURES_BY_YEAR_SQL = INSERT_MART_TICKET_FEATURES_BY_YEAR_SQL + """
                                                        , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.sum_item_qty_scaled, NULL)) as sum_item_qty_scaled_next_yw{i}
                                                        , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.avg_purchase_price, NULL)) as avg_purchase_price_next_yw{i}
                                                        , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_scaled_next_yw{i}
                                                        , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_scaled_next_yw{i}
                                                        , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_scaled_next_yw{i}""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_YEAR_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_YEAR_STATEMENT + ", sum_item_qty_scaled_next_yw{i}, avg_purchase_price_next_yw{i}, avg_item_qty_by_class_scaled_next_yw{i}, avg_item_qty_by_subclass_scaled_next_yw{i}, avg_item_qty_by_model_scaled_next_yw{i}".format(
            i=i)
        DDL_MART_TICKET_FEATURES_BY_YEAR_SQL = DDL_MART_TICKET_FEATURES_BY_YEAR_SQL + """
                                                    , `sum_item_qty_scaled_next_yw{i}` Nullable(Float32)
                                                    , `avg_purchase_price_next_yw{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_class_scaled_next_yw{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_subclass_scaled_next_yw{i}` Nullable(Float32)
                                                    , `avg_item_qty_by_model_scaled_next_yw{i}` Nullable(Float32)""".format(
            i=i)

    DDL_MART_TICKET_FEATURES_BY_YEAR_SQL = DDL_MART_TICKET_FEATURES_BY_YEAR_SQL + """
                                                )
                                                ENGINE = MergeTree()
                                                PARTITION BY week_num
                                                ORDER BY (week_num,
                                                 item)
                                                SETTINGS index_granularity = 8192;
                                                """

    INSERT_MART_TICKET_FEATURES_BY_YEAR_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_YEAR_STATEMENT + ")"
    INSERT_MART_TICKET_FEATURES_BY_YEAR_SQL = INSERT_MART_TICKET_FEATURES_BY_YEAR_STATEMENT + INSERT_MART_TICKET_FEATURES_BY_YEAR_SQL + """
                                                    FROM (
                                                        SELECT
                                                            year,
                                                            week,
                                                            week_num,
                                                            item,
                                                            sum_item_qty_scaled,
                                                            avg_purchase_price,
                                                            avg_item_qty_by_class_scaled,
                                                            avg_item_qty_by_subclass_scaled,
                                                            avg_item_qty_by_model_scaled
                                                        FROM {database_name}.{agg_tickets_attr_by_week_view_name}
                                                        WHERE week_num = {week_num}
                                                        ) src
                                                    INNER JOIN {database_name}.{calendar_table_name} wcft
                                                    ON src.week_num = wcft.week_num
                                                    INNER JOIN (
                                                        SELECT 
                                                            agg.week_num,
                                                            agg.item,
                                                            agg.sum_item_qty_scaled,
                                                            agg.avg_purchase_price,
                                                            agg.avg_item_qty_by_class_scaled,
                                                            agg.avg_item_qty_by_subclass_scaled,
                                                            agg.avg_item_qty_by_model_scaled
                                                        FROM {database_name}.{agg_tickets_attr_by_week_view_name} agg
                                                        WHERE agg.week_num between (SELECT prev_week_num - {last_year_week_features_left} FROM {database_name}.{calendar_table_name} WHERE week_num = {week_num}) 
                                                            AND (SELECT prev_week_num + {last_year_week_features_right} FROM {database_name}.{calendar_table_name} WHERE week_num = {week_num}) 
                                                        ) prev
                                                    ON src.item = prev.item
                                                    GROUP BY 
                                                        src.year,
                                                        src.week,
                                                        src.week_num,
                                                        src.item
                                                    """

    # BUILD ticket_features_by_year
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=ticket_features_by_year_table_name))
    logger.info(DROP_TABLE_SQL.format(database_name=database_name,
                                      table_name=ticket_features_by_year_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DROP_TABLE_SQL.format(database_name=database_name,
                                                                      table_name=ticket_features_by_year_table_name)])

    logger.info(DDL_MART_TICKET_FEATURES_BY_YEAR_SQL.format(database_name=database_name,
                                                            ticket_features_by_year_table_name=ticket_features_by_year_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DDL_MART_TICKET_FEATURES_BY_YEAR_SQL.format(database_name=database_name,
                                                                                            ticket_features_by_year_table_name=ticket_features_by_year_table_name)])

    ticket_features_by_year_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                               table_name=ticket_features_by_year_table_name,
                                                                                                               week_cnt=week_cnt)],
                                                            return_result=True,
                                                            logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=ticket_features_by_year_table_name,
                                                                                       min_week=ticket_features_by_year_min_week,
                                                                                       max_week=max_week))

    for week_num in range(ticket_features_by_year_min_week, max_week + 1):
        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=ticket_features_by_year_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[
                                 INSERT_MART_TICKET_FEATURES_BY_YEAR_SQL.format(database_name=database_name,
                                                                                ticket_features_by_year_table_name=ticket_features_by_year_table_name,
                                                                                agg_tickets_attr_by_week_view_name=agg_tickets_attr_by_week_view_name,
                                                                                calendar_table_name=calendar_table_name,
                                                                                last_year_week_features_left=last_year_week_features_left,
                                                                                last_year_week_features_right=last_year_week_features_right,
                                                                                week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=ticket_features_by_year_table_name))


def load_ticket_features(database_name,
                         ticket_features_table_name,
                         ticket_features_by_week_table_name,
                         ticket_features_by_year_table_name,
                         upsampled_table_name,
                         current_year_week_features_left,
                         current_year_week_features_right,
                         last_year_week_features_left,
                         last_year_week_features_right,
                         week_cnt,
                         dict_week_exception_table_name,
                         **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt})
                            FROM {database_name}.{table_name}
                            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                            FROM {database_name}.{upsampled_table_name}
                            """.format(database_name=database_name,
                                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    DROP_TABLE_SQL = """DROP TABLE IF EXISTS {database_name}.{table_name}"""

    # GENERATE QUERIES FOR MART_TICKET_FEATURES

    INSERT_MART_TICKET_FEATURES_SQL = """
                                            SELECT
                                                src.year,
                                                src.week,
                                                src.week_num,
                                                src.item,
                                                src.sum_item_qty_scaled,
                                                src.avg_purchase_price,
                                                src.avg_item_qty_by_class_scaled,
                                                src.avg_item_qty_by_subclass_scaled,
                                                src.avg_item_qty_by_model_scaled"""

    DDL_MART_TICKET_FEATURES_SQL = """ 
                                                    CREATE TABLE IF NOT EXISTS {database_name}.{ticket_features_table_name}
                                                    (
                                                        `year` UInt32,
                                                        `week` UInt32,
                                                        `week_num` UInt16,
                                                        `item` UInt32,
                                                        `sum_item_qty_scaled` Nullable(Float32),
                                                        `avg_purchase_price` Nullable(Float32),
                                                        `avg_item_qty_by_class_scaled` Nullable(Float32),
                                                        `avg_item_qty_by_subclass_scaled` Nullable(Float32),
                                                        `avg_item_qty_by_model_scaled` Nullable(Float32)"""

    INSERT_MART_TICKET_FEATURES_STATEMENT = """
                                            INSERT INTO {database_name}.{ticket_features_table_name} 
                                                (year, week, week_num, item, sum_item_qty_scaled, avg_purchase_price, avg_item_qty_by_class_scaled, avg_item_qty_by_subclass_scaled, avg_item_qty_by_model_scaled"""

    INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL = """
                                                SELECT
                                                    w.year,
                                                    w.week,
                                                    w.week_num,
                                                    w.item,
                                                    w.sum_item_qty_scaled,
                                                    w.avg_purchase_price,
                                                    w.avg_item_qty_by_class_scaled,
                                                    w.avg_item_qty_by_subclass_scaled,
                                                    w.avg_item_qty_by_model_scaled"""

    for i in range(1, current_year_week_features_left + 1):
        DDL_MART_TICKET_FEATURES_SQL = DDL_MART_TICKET_FEATURES_SQL + """
                                                        , `sum_item_qty_scaled_prev_w{i}` Nullable(Float32)
                                                        , `avg_purchase_price_prev_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_class_prev_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_prev_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_model_prev_w{i}` Nullable(Float32)""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_STATEMENT = INSERT_MART_TICKET_FEATURES_STATEMENT + ", sum_item_qty_scaled_prev_w{i}, avg_purchase_price_prev_w{i}, avg_item_qty_by_class_prev_w{i}, avg_item_qty_by_subclass_prev_w{i}, avg_item_qty_by_model_prev_w{i}".format(
            i=i)

        INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL + """
                                                    , w.sum_item_qty_scaled_prev_w{i}
                                                    , w.avg_purchase_price_prev_w{i}
                                                    , w.avg_item_qty_by_class_prev_w{i}
                                                    , w.avg_item_qty_by_subclass_prev_w{i}
                                                    , w.avg_item_qty_by_model_prev_w{i}""".format(i=i)

        INSERT_MART_TICKET_FEATURES_SQL = INSERT_MART_TICKET_FEATURES_SQL + """
                                                , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, sum_item_qty_scaled_prev_w{i}) as sum_item_qty_scaled_prev_w{i}
                                                , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, avg_purchase_price_prev_w{i}) as avg_purchase_price_prev_w{i}
                                                , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, avg_item_qty_by_class_prev_w{i}) as avg_item_qty_by_class_prev_w{i}
                                                , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, avg_item_qty_by_subclass_prev_w{i}) as avg_item_qty_by_subclass_prev_w{i}
                                                , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, avg_item_qty_by_model_prev_w{i}) as avg_item_qty_by_model_prev_w{i}""".format(i=i)

    for i in range(1, current_year_week_features_right + 1):
        DDL_MART_TICKET_FEATURES_SQL = DDL_MART_TICKET_FEATURES_SQL + """
                                                        , `sum_item_qty_scaled_next_w{i}` Nullable(Float32)
                                                        , `avg_purchase_price_next_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_class_next_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_next_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_model_next_w{i}` Nullable(Float32)""".format(
            i=i)

        INSERT_MART_TICKET_FEATURES_STATEMENT = INSERT_MART_TICKET_FEATURES_STATEMENT + ", sum_item_qty_scaled_next_w{i}, avg_purchase_price_next_w{i}, avg_item_qty_by_class_next_w{i}, avg_item_qty_by_subclass_next_w{i}, avg_item_qty_by_model_next_w{i}".format(
            i=i)

        INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL + """
                                                    , w.sum_item_qty_scaled_next_w{i}
                                                    , w.avg_purchase_price_next_w{i}
                                                    , w.avg_item_qty_by_class_next_w{i}
                                                    , w.avg_item_qty_by_subclass_next_w{i}
                                                    , w.avg_item_qty_by_model_next_w{i}""".format(i=i)

        INSERT_MART_TICKET_FEATURES_SQL = INSERT_MART_TICKET_FEATURES_SQL + """
                                                , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, sum_item_qty_scaled_next_w{i}) as sum_item_qty_scaled_next_w{i}
                                                , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, avg_purchase_price_next_w{i}) as avg_purchase_price_next_w{i}
                                                , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, avg_item_qty_by_class_next_w{i}) as avg_item_qty_by_class_next_w{i}
                                                , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, avg_item_qty_by_subclass_next_w{i}) as avg_item_qty_by_subclass_next_w{i}
                                                , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, avg_item_qty_by_model_next_w{i}) as avg_item_qty_by_model_next_w{i}""".format(i=i)

    DDL_MART_TICKET_FEATURES_SQL = DDL_MART_TICKET_FEATURES_SQL + """
                                                        , `sum_item_qty_scaled_y` Nullable(Float32)
                                                        , `avg_purchase_price_y` Nullable(Float32)
                                                        , `avg_item_qty_by_class_scaled_y` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_scaled_y` Nullable(Float32)
                                                        , `avg_item_qty_by_model_scaled_y` Nullable(Float32)"""

    INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL + """
                                                    , y.sum_item_qty_scaled_y
                                                    , y.avg_purchase_price_y
                                                    , y.avg_item_qty_by_class_scaled_y
                                                    , y.avg_item_qty_by_subclass_scaled_y
                                                    , y.avg_item_qty_by_model_scaled_y"""

    INSERT_MART_TICKET_FEATURES_STATEMENT = INSERT_MART_TICKET_FEATURES_STATEMENT + ", sum_item_qty_scaled_y, avg_purchase_price_y, avg_item_qty_by_class_scaled_y, avg_item_qty_by_subclass_scaled_y, avg_item_qty_by_model_scaled_y"

    INSERT_MART_TICKET_FEATURES_SQL = INSERT_MART_TICKET_FEATURES_SQL + """
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, sum_item_qty_scaled_y) as sum_item_qty_scaled_y
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, avg_purchase_price_y) as avg_purchase_price_y
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, avg_item_qty_by_class_scaled_y) as avg_item_qty_by_class_scaled_y
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, avg_item_qty_by_subclass_scaled_y) as avg_item_qty_by_subclass_scaled_y
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, avg_item_qty_by_model_scaled_y) as avg_item_qty_by_model_scaled_y"""

    for i in range(1, last_year_week_features_left + 1):
        DDL_MART_TICKET_FEATURES_SQL = DDL_MART_TICKET_FEATURES_SQL + """
                                                        , `sum_item_qty_scaled_yw{i}` Nullable(Float32)
                                                        , `avg_purchase_price_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_class_scaled_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_scaled_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_model_scaled_yw{i}` Nullable(Float32)""".format(
            i=i)

        INSERT_MART_TICKET_FEATURES_STATEMENT = INSERT_MART_TICKET_FEATURES_STATEMENT + ", sum_item_qty_scaled_yw{i}, avg_purchase_price_yw{i}, avg_item_qty_by_class_scaled_yw{i}, avg_item_qty_by_subclass_scaled_yw{i}, avg_item_qty_by_model_scaled_yw{i}".format(
            i=i)

        INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL + """
                                                    , y.sum_item_qty_scaled_yw{i}
                                                    , y.avg_purchase_price_yw{i}
                                                    , y.avg_item_qty_by_class_scaled_yw{i}
                                                    , y.avg_item_qty_by_subclass_scaled_yw{i}
                                                    , y.avg_item_qty_by_model_scaled_yw{i}""".format(i=i)

        INSERT_MART_TICKET_FEATURES_SQL = INSERT_MART_TICKET_FEATURES_SQL + """
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, sum_item_qty_scaled_yw{i}) as sum_item_qty_scaled_yw{i}
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, avg_purchase_price_yw{i}) as avg_purchase_price_yw{i}
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, avg_item_qty_by_class_scaled_yw{i}) as avg_item_qty_by_class_scaled_yw{i}
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, avg_item_qty_by_subclass_scaled_yw{i}) as avg_item_qty_by_subclass_scaled_yw{i}
                                                , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, avg_item_qty_by_model_scaled_yw{i}) as avg_item_qty_by_model_scaled_yw{i}""".format(i=i)

    for i in range(1, last_year_week_features_right + 1):
        DDL_MART_TICKET_FEATURES_SQL = DDL_MART_TICKET_FEATURES_SQL + """
                                                        , `sum_item_qty_scaled_next_yw{i}` Nullable(Float32)
                                                        , `avg_purchase_price_next_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_class_scaled_next_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_scaled_next_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_model_scaled_next_yw{i}` Nullable(Float32)""".format(
            i=i)

        INSERT_MART_TICKET_FEATURES_STATEMENT = INSERT_MART_TICKET_FEATURES_STATEMENT + ", sum_item_qty_scaled_next_yw{i}, avg_purchase_price_next_yw{i}, avg_item_qty_by_class_scaled_next_yw{i}, avg_item_qty_by_subclass_scaled_next_yw{i}, avg_item_qty_by_model_scaled_next_yw{i}".format(
            i=i)

        INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL + """
                                                    , y.sum_item_qty_scaled_next_yw{i}
                                                    , y.avg_purchase_price_next_yw{i}
                                                    , y.avg_item_qty_by_class_scaled_next_yw{i}
                                                    , y.avg_item_qty_by_subclass_scaled_next_yw{i}
                                                    , y.avg_item_qty_by_model_scaled_next_yw{i}""".format(
            i=i)

        INSERT_MART_TICKET_FEATURES_SQL = INSERT_MART_TICKET_FEATURES_SQL + """
                                                , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, sum_item_qty_scaled_next_yw{i}) as sum_item_qty_scaled_next_yw{i}
                                                , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, avg_purchase_price_next_yw{i}) as avg_purchase_price_next_yw{i}
                                                , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, avg_item_qty_by_class_scaled_next_yw{i}) as avg_item_qty_by_class_scaled_next_yw{i}
                                                , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, avg_item_qty_by_subclass_scaled_next_yw{i}) as avg_item_qty_by_subclass_scaled_next_yw{i}
                                                , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, avg_item_qty_by_model_scaled_next_yw{i}) as avg_item_qty_by_model_scaled_next_yw{i}""".format(
            i=i)

    DDL_MART_TICKET_FEATURES_SQL = DDL_MART_TICKET_FEATURES_SQL + """
                                                    )
                                                    ENGINE = MergeTree()
                                                    PARTITION BY week_num
                                                    ORDER BY (week_num,
                                                     item)
                                                    SETTINGS index_granularity = 8192;
                                                    """

    INSERT_MART_TICKET_FEATURES_STATEMENT = INSERT_MART_TICKET_FEATURES_STATEMENT + ")"

    INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL + """
                                                FROM (
                                                    SELECT * 
                                                    FROM {database_name}.{ticket_features_by_week_table_name}
                                                    WHERE week_num = {week_num}
                                                ) w
                                                LEFT JOIN (
                                                    SELECT * 
                                                    FROM {database_name}.{ticket_features_by_year_table_name}
                                                    WHERE week_num = {week_num}
                                                ) y
                                                ON w.week_num = y.week_num
                                                    AND w.item = y.item"""

    INSERT_MART_TICKET_FEATURES_SQL = INSERT_MART_TICKET_FEATURES_STATEMENT + INSERT_MART_TICKET_FEATURES_SQL + """
                                        FROM (""" + INSERT_SUBQUERY_MART_TICKET_FEATURES_SQL + """
                                            ) src
                                        LEFT JOIN {database_name}.{dict_week_exception_table_name} exc
                                            ON src.week_num = exc.week_num
                                        ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_prev_w1
                                            ON src.year = exc_prev_w1.year AND toUInt32(src.week_num) <= exc_prev_w1.week_num + {current_year_week_features_left}
                                        ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_prev_w2
                                            ON src.year = exc_prev_w2.year AND src.week_num >= exc_prev_w2.week_num
                                        ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_next_w1
                                            ON src.year = exc_next_w1.year AND toInt32(src.week_num) >= exc_next_w1.week_num - {current_year_week_features_right}
                                        ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_next_w2
                                            ON src.year = exc_next_w2.year AND src.week_num <= exc_next_w2.week_num
                                        ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_prev_yw1
                                            ON toUInt64(src.year) = exc_prev_yw1.year + 1 AND toUInt32(src.week_num) <= exc_prev_yw1.next_week_num + {last_year_week_features_left}
                                        ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_prev_yw2
                                            ON toUInt64(src.year) = exc_prev_yw2.year + 1 AND src.week_num >= exc_prev_yw2.next_week_num
                                        ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_next_yw1
                                            ON toUInt64(src.year) = exc_next_yw1.year + 1 AND toInt32(src.week_num) >= exc_next_yw1.next_week_num - {last_year_week_features_right}
                                        ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_next_yw2
                                            ON toUInt64(src.year) = exc_next_yw2.year + 1 AND src.week_num <= exc_next_yw2.next_week_num
                                        WHERE src.week_num <> exc.week_num 
                                        """

    # BUILD ticket_features
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=ticket_features_table_name))
    logger.info(DROP_TABLE_SQL.format(database_name=database_name,
                                      table_name=ticket_features_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DROP_TABLE_SQL.format(database_name=database_name,
                                                                      table_name=ticket_features_table_name)])

    logger.info(DDL_MART_TICKET_FEATURES_SQL.format(database_name=database_name,
                                                    ticket_features_table_name=ticket_features_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DDL_MART_TICKET_FEATURES_SQL.format(database_name=database_name,
                                                                                    ticket_features_table_name=ticket_features_table_name)])

    ticket_features_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                       table_name=ticket_features_table_name,
                                                                                                       week_cnt=week_cnt)],
                                                    return_result=True,
                                                    logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=ticket_features_table_name,
                                                                                       min_week=ticket_features_min_week,
                                                                                       max_week=max_week))

    for week_num in range(ticket_features_min_week, max_week + 1):
        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=ticket_features_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[
                                 INSERT_MART_TICKET_FEATURES_SQL.format(database_name=database_name,
                                                                        ticket_features_table_name=ticket_features_table_name,
                                                                        ticket_features_by_week_table_name=ticket_features_by_week_table_name,
                                                                        ticket_features_by_year_table_name=ticket_features_by_year_table_name,
                                                                        dict_week_exception_table_name=dict_week_exception_table_name,
                                                                        current_year_week_features_left=current_year_week_features_left,
                                                                        current_year_week_features_right=current_year_week_features_right,
                                                                        last_year_week_features_left=last_year_week_features_left,
                                                                        last_year_week_features_right=last_year_week_features_right,
                                                                        week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=ticket_features_table_name))


def load_agg_tickets_attr_by_store(database_name,
                                   agg_tickets_attr_by_store_table_name,
                                   agg_tickets_by_store_table_name,
                                   avg_week_tickets_by_class_view_name,
                                   avg_week_tickets_by_subclass_view_name,
                                   avg_week_tickets_by_model_view_name,
                                   upsampled_table_name,
                                   week_cnt,
                                   agg_tickets_attr_by_store_max_val_table_name,
                                   **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                            FROM {database_name}.{table_name}
                            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                            FROM {database_name}.{upsampled_table_name}
                            """.format(database_name=database_name,
                                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    INSERT_MART_AGG_TICKETS_ATTR_BY_STORE_SQL = """
                                                INSERT INTO {database_name}.{agg_tickets_attr_by_store_table_name}
                                                    (year, week, week_num, store, item, dept_class_subclass, model, week_item_qty, avg_purchase_price, avg_item_qty_by_class_scaled, avg_item_qty_by_subclass_scaled, avg_item_qty_by_model_scaled)
                                                SELECT 
                                                    src.year,
                                                    src.week,
                                                    src.week_num,
                                                    src.store,
                                                    src.item,
                                                    src.dept_class_subclass,
                                                    src.model,
                                                    src.week_item_qty,
                                                    src.sum_purchase_price/src.count_tickets as avg_purchase_price,
                                                    c.avg_item_qty_by_class_scaled,
                                                    s.avg_item_qty_by_subclass_scaled,
                                                    m.avg_item_qty_by_model_scaled
                                                FROM {database_name}.{agg_tickets_by_store_table_name} src
                                                LEFT JOIN (
                                                    SELECT 
                                                        week_num,
                                                        dept_class,
                                                        avg_item_qty_by_class_scaled
                                                    FROM {database_name}.{avg_week_tickets_by_class_view_name}
                                                    WHERE week_num = {week_num}
                                                ) c
                                                ON arrayStringConcat(arraySlice(splitByChar('|', src.dept_class_subclass), 1, 2), '|') = c.dept_class 
                                                    AND src.week_num = c.week_num
                                                LEFT JOIN (
                                                    SELECT 
                                                        week_num,
                                                        dept_class_subclass,
                                                        avg_item_qty_by_subclass_scaled
                                                    FROM {database_name}.{avg_week_tickets_by_subclass_view_name}
                                                    WHERE week_num = {week_num}
                                                ) s
                                                ON src.dept_class_subclass = s.dept_class_subclass 
                                                    AND src.week_num = s.week_num
                                                LEFT JOIN (
                                                    SELECT 
                                                        week_num,
                                                        model,
                                                        avg_item_qty_by_model_scaled
                                                    FROM {database_name}.{avg_week_tickets_by_model_view_name}
                                                    WHERE week_num = {week_num}
                                                ) m
                                                ON src.model = m.model 
                                                    AND src.week_num = m.week_num
                                                WHERE src.week_num = {week_num}
                                                """

    INSERT_WORK_AGG_TICKETS_BY_STORE_MAX_VAL_SQL = """
                                                    INSERT INTO {database_name}.{agg_tickets_attr_by_store_max_val_table_name}
                                                        (item, store, max_week_item_qty)
                                                    SELECT 
                                                        item,
                                                        store,
                                                        max(week_item_qty) as max_week_item_qty
                                                    FROM {database_name}.{agg_tickets_attr_by_store_table_name}
                                                    GROUP BY item, store
                                                    """

    # BUILD agg_tickets_attr_by_store
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=agg_tickets_attr_by_store_table_name))
    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=agg_tickets_attr_by_store_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=agg_tickets_attr_by_store_table_name)])

    agg_tickets_attr_by_store_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                                 table_name=agg_tickets_attr_by_store_table_name,
                                                                                                                 week_cnt=week_cnt)],
                                                              return_result=True,
                                                              logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=agg_tickets_attr_by_store_table_name,
                                                                                       min_week=agg_tickets_attr_by_store_min_week,
                                                                                       max_week=max_week))

    for week_num in range(agg_tickets_attr_by_store_min_week, max_week + 1):
        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=agg_tickets_attr_by_store_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[
                                 INSERT_MART_AGG_TICKETS_ATTR_BY_STORE_SQL.format(database_name=database_name,
                                                                                  agg_tickets_attr_by_store_table_name=agg_tickets_attr_by_store_table_name,
                                                                                  agg_tickets_by_store_table_name=agg_tickets_by_store_table_name,
                                                                                  avg_week_tickets_by_class_view_name=avg_week_tickets_by_class_view_name,
                                                                                  avg_week_tickets_by_subclass_view_name=avg_week_tickets_by_subclass_view_name,
                                                                                  avg_week_tickets_by_model_view_name=avg_week_tickets_by_model_view_name,
                                                                                  week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=agg_tickets_attr_by_store_table_name))

    # BUILD agg_tickets_by_store_max_val

    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=agg_tickets_attr_by_store_max_val_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=agg_tickets_attr_by_store_max_val_table_name)])

    logger.info(
        "inserting data into {database_name}.{table_name}".format(database_name=database_name,
                                                                  table_name=agg_tickets_attr_by_store_max_val_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[
                             INSERT_WORK_AGG_TICKETS_BY_STORE_MAX_VAL_SQL.format(database_name=database_name,
                                                                                 agg_tickets_attr_by_store_max_val_table_name=agg_tickets_attr_by_store_max_val_table_name,
                                                                                 agg_tickets_attr_by_store_table_name=agg_tickets_attr_by_store_table_name)])

    logger.info(
        "build {database_name}.{avg_week_tickets_by_model_table_name} end".format(database_name=database_name,
                                                                                  avg_week_tickets_by_model_table_name=agg_tickets_attr_by_store_max_val_table_name))


def load_ticket_features_by_store_week(database_name,
                                       ticket_features_by_store_week_table_name,
                                       agg_tickets_attr_by_store_view_name,
                                       upsampled_table_name,
                                       current_year_week_features_left,
                                       current_year_week_features_right,
                                       week_cnt,
                                       **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                            FROM {database_name}.{table_name}
                            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                            FROM {database_name}.{upsampled_table_name}
                            """.format(database_name=database_name,
                                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    DROP_TABLE_SQL = """DROP TABLE IF EXISTS {database_name}.{table_name}"""

    # GENERATE QUERIES FOR MART_TICKET_FEATURES_BY_STORE_WEEK_SQL

    DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL = """ 
                                                    CREATE TABLE IF NOT EXISTS {database_name}.{ticket_features_by_store_week_table_name}
                                                    (
                                                        `year` UInt32,
                                                        `week` UInt32,
                                                        `week_num` UInt16,
                                                        `item` UInt32,
                                                        `store` Int16,
                                                        `week_item_qty_scaled` Nullable(Float32),
                                                        `avg_purchase_price` Nullable(Float32),
                                                        `avg_item_qty_by_class_scaled` Nullable(Float32),
                                                        `avg_item_qty_by_subclass_scaled` Nullable(Float32),
                                                        `avg_item_qty_by_model_scaled` Nullable(Float32)"""

    INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_STATEMENT = """
                                                        INSERT INTO {database_name}.{ticket_features_by_store_week_table_name} 
                                                            (year, week, week_num, item, store, week_item_qty_scaled, avg_purchase_price, avg_item_qty_by_class_scaled, avg_item_qty_by_subclass_scaled, avg_item_qty_by_model_scaled"""

    INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL = """
                                                        SELECT
                                                            src.year,
                                                            src.week,
                                                            src.week_num,
                                                            src.item,
                                                            src.store,
                                                            src.week_item_qty_scaled,
                                                            src.avg_purchase_price,
                                                            src.avg_item_qty_by_class_scaled,
                                                            src.avg_item_qty_by_subclass_scaled,
                                                            src.avg_item_qty_by_model_scaled"""

    for i in range(1, current_year_week_features_left + 1):
        INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL + """
                                                            , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.week_item_qty_scaled, NULL)) as week_item_qty_scaled_prev_w{i}
                                                            , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.avg_purchase_price, NULL)) as avg_purchase_price_prev_w{i}
                                                            , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_prev_w{i}
                                                            , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_prev_w{i}
                                                            , sum(if(toUInt16(src.week_num - {i}) = prev.week_num, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_prev_w{i}""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_STATEMENT + ", week_item_qty_scaled_prev_w{i}, avg_purchase_price_prev_w{i}, avg_item_qty_by_class_prev_w{i}, avg_item_qty_by_subclass_prev_w{i}, avg_item_qty_by_model_prev_w{i}".format(
            i=i)
        DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL + """
                                                        , `week_item_qty_scaled_prev_w{i}` Nullable(Float32)
                                                        , `avg_purchase_price_prev_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_class_prev_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_prev_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_model_prev_w{i}` Nullable(Float32)""".format(
            i=i)

    for i in range(1, current_year_week_features_right + 1):
        INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL + """
                                                            , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.week_item_qty_scaled, NULL)) as week_item_qty_scaled_next_w{i}
                                                            , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.avg_purchase_price, NULL)) as avg_purchase_price_next_w{i}
                                                            , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_next_w{i}
                                                            , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_next_w{i}
                                                            , sum(if(toUInt16(src.week_num + {i}) = prev.week_num, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_next_w{i}""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_STATEMENT + ", week_item_qty_scaled_next_w{i}, avg_purchase_price_next_w{i}, avg_item_qty_by_class_next_w{i}, avg_item_qty_by_subclass_next_w{i}, avg_item_qty_by_model_next_w{i}".format(
            i=i)
        DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL + """
                                                        , `week_item_qty_scaled_next_w{i}` Nullable(Float32)
                                                        , `avg_purchase_price_next_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_class_next_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_next_w{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_model_next_w{i}` Nullable(Float32)""".format(
            i=i)

    DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL + """
                                                    )
                                                    ENGINE = MergeTree()
                                                    PARTITION BY week_num
                                                    ORDER BY (week_num,
                                                     item, store)
                                                    SETTINGS index_granularity = 8192;
                                                    """
    INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_STATEMENT + ")"
    INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_STATEMENT + INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL + """
                                                        FROM (
                                                            SELECT
                                                                year,
                                                                week,
                                                                week_num,
                                                                item,
                                                                store,
                                                                week_item_qty_scaled,
                                                                avg_purchase_price,
                                                                avg_item_qty_by_class_scaled,
                                                                avg_item_qty_by_subclass_scaled,
                                                                avg_item_qty_by_model_scaled
                                                            FROM {database_name}.{agg_tickets_attr_by_store_view_name}
                                                            WHERE week_num = {week_num}
                                                            ) src
                                                        LEFT JOIN (
                                                            SELECT 
                                                                year,
                                                                week,
                                                                week_num,
                                                                item,
                                                                store,
                                                                week_item_qty_scaled,
                                                                avg_purchase_price,
                                                                avg_item_qty_by_class_scaled,
                                                                avg_item_qty_by_subclass_scaled,
                                                                avg_item_qty_by_model_scaled
                                                            FROM {database_name}.{agg_tickets_attr_by_store_view_name} 
                                                            WHERE week_num between ({week_num} - {current_year_week_features_left}) AND ({week_num} + {current_year_week_features_right})
                                                            ) prev
                                                        ON src.item = prev.item
                                                            AND src.store = prev.store    
                                                        GROUP BY 
                                                            src.year,
                                                            src.week,
                                                            src.week_num,
                                                            src.item,
                                                            src.store,
                                                            src.week_item_qty_scaled,
                                                            src.avg_purchase_price,
                                                            src.avg_item_qty_by_class_scaled,
                                                            src.avg_item_qty_by_subclass_scaled,
                                                            src.avg_item_qty_by_model_scaled
                                                        """

    # BUILD ticket_features_by_store_week
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=ticket_features_by_store_week_table_name))
    logger.info(DROP_TABLE_SQL.format(database_name=database_name,
                                      table_name=ticket_features_by_store_week_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DROP_TABLE_SQL.format(database_name=database_name,
                                                                      table_name=ticket_features_by_store_week_table_name)])

    logger.info(DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL.format(database_name=database_name,
                                                                  ticket_features_by_store_week_table_name=ticket_features_by_store_week_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[
                             DDL_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL.format(database_name=database_name,
                                                                               ticket_features_by_store_week_table_name=ticket_features_by_store_week_table_name)])

    ticket_features_by_store_week_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                                     table_name=ticket_features_by_store_week_table_name,
                                                                                                                     week_cnt=week_cnt)],
                                                                  return_result=True,
                                                                  logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=ticket_features_by_store_week_table_name,
                                                                                       min_week=ticket_features_by_store_week_min_week,
                                                                                       max_week=max_week))

    logger.info(
        INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL.format(database_name=database_name,
                                                             ticket_features_by_store_week_table_name=ticket_features_by_store_week_table_name,
                                                             agg_tickets_attr_by_store_view_name=agg_tickets_attr_by_store_view_name,
                                                             current_year_week_features_left=current_year_week_features_left,
                                                             current_year_week_features_right=current_year_week_features_right,
                                                             week_num='0'))

    for week_num in range(ticket_features_by_store_week_min_week, max_week + 1):
        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=ticket_features_by_store_week_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[
                                 INSERT_MART_TICKET_FEATURES_BY_STORE_WEEK_SQL.format(database_name=database_name,
                                                                                      ticket_features_by_store_week_table_name=ticket_features_by_store_week_table_name,
                                                                                      agg_tickets_attr_by_store_view_name=agg_tickets_attr_by_store_view_name,
                                                                                      current_year_week_features_left=current_year_week_features_left,
                                                                                      current_year_week_features_right=current_year_week_features_right,
                                                                                      week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=ticket_features_by_store_week_table_name))


def load_ticket_features_by_store_year(database_name,
                                       ticket_features_by_store_year_table_name,
                                       agg_tickets_attr_by_store_view_name,
                                       upsampled_table_name,
                                       calendar_table_name,
                                       last_year_week_features_left,
                                       last_year_week_features_right,
                                       week_cnt,
                                       **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                            FROM {database_name}.{table_name}
                            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                            FROM {database_name}.{upsampled_table_name}
                            """.format(database_name=database_name,
                                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    DROP_TABLE_SQL = """DROP TABLE IF EXISTS {database_name}.{table_name}"""

    # GENERATE QUERIES FOR MART_TICKET_FEATURES_BY_STORE_YEAR

    DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL = """ 
                                                    CREATE TABLE IF NOT EXISTS {database_name}.{ticket_features_by_store_year_table_name}
                                                    (
                                                        `year` UInt32,
                                                        `week` UInt32,
                                                        `week_num` UInt16,
                                                        `item` UInt32,
                                                        `store` Int16,
                                                        `week_item_qty_scaled_y` Nullable(Float32),
                                                        `avg_purchase_price_y` Nullable(Float32),
                                                        `avg_item_qty_by_class_scaled_y` Nullable(Float32),
                                                        `avg_item_qty_by_subclass_scaled_y` Nullable(Float32),
                                                        `avg_item_qty_by_model_scaled_y` Nullable(Float32)"""

    INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_STATEMENT = """
                                                        INSERT INTO {database_name}.{ticket_features_by_store_year_table_name} 
                                                            (year, week, week_num, item, store, week_item_qty_scaled_y, avg_purchase_price_y, avg_item_qty_by_class_scaled_y, avg_item_qty_by_subclass_scaled_y, avg_item_qty_by_model_scaled_y"""

    INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL = """
                                                        SELECT
                                                            src.year,
                                                            src.week,
                                                            src.week_num,
                                                            src.item,
                                                            src.store,
                                                            sum(if(prev.week_num = wcft.prev_week_num, prev.week_item_qty_scaled, NULL)) as week_item_qty_scaled_y,
                                                            sum(if(prev.week_num = wcft.prev_week_num, prev.avg_purchase_price, NULL)) as avg_purchase_price_y,
                                                            sum(if(prev.week_num = wcft.prev_week_num, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_scaled_y,
                                                            sum(if(prev.week_num = wcft.prev_week_num, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_scaled_y,
                                                            sum(if(prev.week_num = wcft.prev_week_num, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_scaled_y"""
    for i in range(1, last_year_week_features_left + 1):
        INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL + """
                                                            , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.week_item_qty_scaled, NULL)) as week_item_qty_scaled_yw{i}
                                                            , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.avg_purchase_price, NULL)) as avg_purchase_price_yw{i}
                                                            , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_scaled_yw{i}
                                                            , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_scaled_yw{i}
                                                            , sum(if(prev.week_num = wcft.prev_week_num - {i}, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_scaled_yw{i}""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_STATEMENT + ", week_item_qty_scaled_yw{i}, avg_purchase_price_yw{i}, avg_item_qty_by_class_scaled_yw{i}, avg_item_qty_by_subclass_scaled_yw{i}, avg_item_qty_by_model_scaled_yw{i}".format(
            i=i)
        DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL + """
                                                        , `week_item_qty_scaled_yw{i}` Nullable(Float32)
                                                        , `avg_purchase_price_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_class_scaled_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_scaled_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_model_scaled_yw{i}` Nullable(Float32)""".format(
            i=i)

    for i in range(1, last_year_week_features_right + 1):
        INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL + """
                                                            , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.week_item_qty_scaled, NULL)) as week_item_qty_scaled_next_yw{i}
                                                            , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.avg_purchase_price, NULL)) as avg_purchase_price_next_yw{i}
                                                            , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.avg_item_qty_by_class_scaled, NULL)) as avg_item_qty_by_class_scaled_next_yw{i}
                                                            , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.avg_item_qty_by_subclass_scaled, NULL)) as avg_item_qty_by_subclass_scaled_next_yw{i}
                                                            , sum(if(prev.week_num = wcft.prev_week_num + {i}, prev.avg_item_qty_by_model_scaled, NULL)) as avg_item_qty_by_model_scaled_next_yw{i}""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_STATEMENT + ", week_item_qty_scaled_next_yw{i}, avg_purchase_price_next_yw{i}, avg_item_qty_by_class_scaled_next_yw{i}, avg_item_qty_by_subclass_scaled_next_yw{i}, avg_item_qty_by_model_scaled_next_yw{i}".format(
            i=i)
        DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL + """
                                                        , `week_item_qty_scaled_next_yw{i}` Nullable(Float32)
                                                        , `avg_purchase_price_next_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_class_scaled_next_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_subclass_scaled_next_yw{i}` Nullable(Float32)
                                                        , `avg_item_qty_by_model_scaled_next_yw{i}` Nullable(Float32)""".format(
            i=i)

    DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL + """
                                                    )
                                                    ENGINE = MergeTree()
                                                    PARTITION BY week_num
                                                    ORDER BY (week_num,
                                                     item, store)
                                                    SETTINGS index_granularity = 8192;
                                                    """

    INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_STATEMENT + ")"
    INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_STATEMENT + INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL + """
                                                        FROM (
                                                            SELECT
                                                                year,
                                                                week,
                                                                week_num,
                                                                item,
                                                                store,
                                                                week_item_qty_scaled,
                                                                avg_purchase_price,
                                                                avg_item_qty_by_class_scaled,
                                                                avg_item_qty_by_subclass_scaled,
                                                                avg_item_qty_by_model_scaled
                                                            FROM {database_name}.{agg_tickets_attr_by_store_view_name}
                                                            WHERE week_num = {week_num}
                                                            ) src
                                                        INNER JOIN {database_name}.{calendar_table_name} wcft
                                                        ON src.week_num = wcft.week_num
                                                        INNER JOIN (
                                                            SELECT 
                                                                agg.week_num,
                                                                agg.item,
                                                                agg.store,
                                                                agg.week_item_qty_scaled,
                                                                agg.avg_purchase_price,
                                                                agg.avg_item_qty_by_class_scaled,
                                                                agg.avg_item_qty_by_subclass_scaled,
                                                                agg.avg_item_qty_by_model_scaled
                                                            FROM {database_name}.{agg_tickets_attr_by_store_view_name} agg
                                                            WHERE agg.week_num between (SELECT prev_week_num - {last_year_week_features_left} FROM {database_name}.{calendar_table_name} WHERE week_num = {week_num}) 
                                                                AND (SELECT prev_week_num + {last_year_week_features_right} FROM {database_name}.{calendar_table_name} WHERE week_num = {week_num}) 
                                                            ) prev
                                                        ON src.item = prev.item
                                                            AND src.store = prev.store
                                                        GROUP BY 
                                                            src.year,
                                                            src.week,
                                                            src.week_num,
                                                            src.item,
                                                            src.store
                                                        """


    # BUILD ticket_features_by_store_year
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=ticket_features_by_store_year_table_name))
    logger.info(DROP_TABLE_SQL.format(database_name=database_name,
                                      table_name=ticket_features_by_store_year_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DROP_TABLE_SQL.format(database_name=database_name,
                                                                      table_name=ticket_features_by_store_year_table_name)])

    logger.info(DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL.format(database_name=database_name,
                                                                  ticket_features_by_store_year_table_name=ticket_features_by_store_year_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[
                             DDL_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL.format(database_name=database_name,
                                                                               ticket_features_by_store_year_table_name=ticket_features_by_store_year_table_name)])

    ticket_features_by_store_year_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                                     table_name=ticket_features_by_store_year_table_name,
                                                                                                                     week_cnt=week_cnt)],
                                                                  return_result=True,
                                                                  logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=ticket_features_by_store_year_table_name,
                                                                                       min_week=ticket_features_by_store_year_min_week,
                                                                                       max_week=max_week))

    for week_num in range(ticket_features_by_store_year_min_week, max_week + 1):
        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=ticket_features_by_store_year_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[
                                 INSERT_MART_TICKET_FEATURES_BY_STORE_YEAR_SQL.format(database_name=database_name,
                                                                                      ticket_features_by_store_year_table_name=ticket_features_by_store_year_table_name,
                                                                                      agg_tickets_attr_by_store_view_name=agg_tickets_attr_by_store_view_name,
                                                                                      calendar_table_name=calendar_table_name,
                                                                                      last_year_week_features_left=last_year_week_features_left,
                                                                                      last_year_week_features_right=last_year_week_features_right,
                                                                                      week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=ticket_features_by_store_year_table_name))


def load_ticket_features_by_store(database_name,
                                  ticket_features_by_store_table_name,
                                  ticket_features_by_store_week_table_name,
                                  ticket_features_by_store_year_table_name,
                                  upsampled_table_name,
                                  dict_week_exception_table_name,
                                  current_year_week_features_left,
                                  current_year_week_features_right,
                                  last_year_week_features_left,
                                  last_year_week_features_right,
                                  week_cnt,
                                  **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # первая неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(min(sell_date))). Если в таблице ничего нет, то грузим с 2018-01-01
    GET_MIN_WEEK_NUM_SQL = """
                            SELECT if(count(*) = 0, toRelativeWeekNum(toMonday(toDate('2018-01-01'))), max(week_num) - {week_cnt}) 
                            FROM {database_name}.{table_name}
                            """

    # последняя неделя, за которую нужно грузить данные SELECT toRelativeWeekNum(toMonday(max(sell_date))) FROM salesfs.work_tickets_agg_day_store_item_upsampled
    GET_MAX_WEEK_NUM_SQL = """
                            SELECT toRelativeWeekNum(toMonday(max(sell_date))) - 1
                            FROM {database_name}.{upsampled_table_name}
                            """.format(database_name=database_name,
                                       upsampled_table_name=upsampled_table_name)

    max_week = run_clickhouse_query(logger=logger,
                                    return_result=True,
                                    clickhouse_query_list=[GET_MAX_WEEK_NUM_SQL])[0][0][0]

    DROP_TABLE_SQL = """DROP TABLE IF EXISTS {database_name}.{table_name}"""

    # GENERATE QUERIES FOR MART_TICKET_FEATURES
    DDL_MART_TICKET_FEATURES_BY_STORE_SQL = """ 
                                                        CREATE TABLE IF NOT EXISTS {database_name}.{ticket_features_by_store_table_name}
                                                        (
                                                            `year` UInt32,
                                                            `week` UInt32,
                                                            `week_num` UInt16,
                                                            `item` UInt32,
                                                            `store` Int16,
                                                            `week_item_qty_scaled` Nullable(Float32),
                                                            `avg_purchase_price` Nullable(Float32),
                                                            `avg_item_qty_by_class_scaled` Nullable(Float32),
                                                            `avg_item_qty_by_subclass_scaled` Nullable(Float32),
                                                            `avg_item_qty_by_model_scaled` Nullable(Float32)"""

    INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT = """
                                                   INSERT INTO {database_name}.{ticket_features_by_store_table_name} 
                                                       (year, week, week_num, item, store, week_item_qty_scaled, avg_purchase_price, avg_item_qty_by_class_scaled, avg_item_qty_by_subclass_scaled, avg_item_qty_by_model_scaled"""

    INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL = """
                                                    SELECT
                                                        w.year,
                                                        w.week,
                                                        w.week_num,
                                                        w.item,
                                                        w.store,
                                                        w.week_item_qty_scaled,
                                                        w.avg_purchase_price,
                                                        w.avg_item_qty_by_class_scaled,
                                                        w.avg_item_qty_by_subclass_scaled,
                                                        w.avg_item_qty_by_model_scaled"""

    INSERT_MART_TICKET_FEATURES_BY_STORE_SQL = """
                                                SELECT
                                                    src.year,
                                                    src.week,
                                                    src.week_num,
                                                    src.item,
                                                    src.store,
                                                    src.week_item_qty_scaled,
                                                    src.avg_purchase_price,
                                                    src.avg_item_qty_by_class_scaled,
                                                    src.avg_item_qty_by_subclass_scaled,
                                                    src.avg_item_qty_by_model_scaled"""

    for i in range(1, current_year_week_features_left + 1):
        DDL_MART_TICKET_FEATURES_BY_STORE_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                            , `week_item_qty_scaled_prev_w{i}` Nullable(Float32)
                                                            , `avg_purchase_price_prev_w{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_class_prev_w{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_subclass_prev_w{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_model_prev_w{i}` Nullable(Float32)""".format(
            i=i)
        INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT + ", week_item_qty_scaled_prev_w{i}, avg_purchase_price_prev_w{i}, avg_item_qty_by_class_prev_w{i}, avg_item_qty_by_subclass_prev_w{i}, avg_item_qty_by_model_prev_w{i}".format(
            i=i)

        INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                        , w.week_item_qty_scaled_prev_w{i}
                                                        , w.avg_purchase_price_prev_w{i}
                                                        , w.avg_item_qty_by_class_prev_w{i}
                                                        , w.avg_item_qty_by_subclass_prev_w{i}
                                                        , w.avg_item_qty_by_model_prev_w{i}""".format(i=i)

        INSERT_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                    , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, week_item_qty_scaled_prev_w{i}) as week_item_qty_scaled_prev_w{i}
                                                    , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, avg_purchase_price_prev_w{i}) as avg_purchase_price_prev_w{i}
                                                    , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, avg_item_qty_by_class_prev_w{i}) as avg_item_qty_by_class_prev_w{i}
                                                    , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, avg_item_qty_by_subclass_prev_w{i}) as avg_item_qty_by_subclass_prev_w{i}
                                                    , if(exc_prev_w1.week_num <> 0 and src.week_num - exc_prev_w2.week_num <= {i} and src.week_num - exc_prev_w1.week_num >= {i}, NULL, avg_item_qty_by_model_prev_w{i}) as avg_item_qty_by_model_prev_w{i}""".format(
            i=i)

    for i in range(1, current_year_week_features_right + 1):

        DDL_MART_TICKET_FEATURES_BY_STORE_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                            , `week_item_qty_scaled_next_w{i}` Nullable(Float32)
                                                            , `avg_purchase_price_next_w{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_class_next_w{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_subclass_next_w{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_model_next_w{i}` Nullable(Float32)""".format(
            i=i)

        INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT + ", week_item_qty_scaled_next_w{i}, avg_purchase_price_next_w{i}, avg_item_qty_by_class_next_w{i}, avg_item_qty_by_subclass_next_w{i}, avg_item_qty_by_model_next_w{i}".format(
            i=i)

        INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                        , w.week_item_qty_scaled_next_w{i}
                                                        , w.avg_purchase_price_next_w{i}
                                                        , w.avg_item_qty_by_class_next_w{i}
                                                        , w.avg_item_qty_by_subclass_next_w{i}
                                                        , w.avg_item_qty_by_model_next_w{i}""".format(i=i)

        INSERT_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                    , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, week_item_qty_scaled_next_w{i}) as week_item_qty_scaled_next_w{i}
                                                    , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, avg_purchase_price_next_w{i}) as avg_purchase_price_next_w{i}
                                                    , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, avg_item_qty_by_class_next_w{i}) as avg_item_qty_by_class_next_w{i}
                                                    , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, avg_item_qty_by_subclass_next_w{i}) as avg_item_qty_by_subclass_next_w{i}
                                                    , if(exc_next_w1.week_num <> 0 and src.week_num - exc_next_w2.week_num >= -{i} and src.week_num - exc_next_w1.week_num <= -{i}, NULL, avg_item_qty_by_model_next_w{i}) as avg_item_qty_by_model_next_w{i}""".format(
            i=i)

    DDL_MART_TICKET_FEATURES_BY_STORE_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                            , `week_item_qty_scaled_y` Nullable(Float32)
                                                            , `avg_purchase_price_y` Nullable(Float32)
                                                            , `avg_item_qty_by_class_scaled_y` Nullable(Float32)
                                                            , `avg_item_qty_by_subclass_scaled_y` Nullable(Float32)
                                                            , `avg_item_qty_by_model_scaled_y` Nullable(Float32)"""

    INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                            , y.week_item_qty_scaled_y
                                                            , y.avg_purchase_price_y
                                                            , y.avg_item_qty_by_class_scaled_y
                                                            , y.avg_item_qty_by_subclass_scaled_y
                                                            , y.avg_item_qty_by_model_scaled_y"""

    INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT + ", week_item_qty_scaled_y, avg_purchase_price_y, avg_item_qty_by_class_scaled_y, avg_item_qty_by_subclass_scaled_y, avg_item_qty_by_model_scaled_y"

    INSERT_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, week_item_qty_scaled_y) as week_item_qty_scaled_y
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, avg_purchase_price_y) as avg_purchase_price_y
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, avg_item_qty_by_class_scaled_y) as avg_item_qty_by_class_scaled_y
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, avg_item_qty_by_subclass_scaled_y) as avg_item_qty_by_subclass_scaled_y
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= 0 and src.week_num - exc_prev_yw1.next_week_num >= 0, NULL, avg_item_qty_by_model_scaled_y) as avg_item_qty_by_model_scaled_y"""

    for i in range(1, last_year_week_features_left + 1):
        DDL_MART_TICKET_FEATURES_BY_STORE_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                            , `week_item_qty_scaled_yw{i}` Nullable(Float32)
                                                            , `avg_purchase_price_yw{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_class_scaled_yw{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_subclass_scaled_yw{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_model_scaled_yw{i}` Nullable(Float32)""".format(
            i=i)

        INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT + ", week_item_qty_scaled_yw{i}, avg_purchase_price_yw{i}, avg_item_qty_by_class_scaled_yw{i}, avg_item_qty_by_subclass_scaled_yw{i}, avg_item_qty_by_model_scaled_yw{i}".format(
            i=i)

        INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                        , y.week_item_qty_scaled_yw{i}
                                                        , y.avg_purchase_price_yw{i}
                                                        , y.avg_item_qty_by_class_scaled_yw{i}
                                                        , y.avg_item_qty_by_subclass_scaled_yw{i}
                                                        , y.avg_item_qty_by_model_scaled_yw{i}""".format(i=i)

        INSERT_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, week_item_qty_scaled_yw{i}) as week_item_qty_scaled_yw{i}
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, avg_purchase_price_yw{i}) as avg_purchase_price_yw{i}
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, avg_item_qty_by_class_scaled_yw{i}) as avg_item_qty_by_class_scaled_yw{i}
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, avg_item_qty_by_subclass_scaled_yw{i}) as avg_item_qty_by_subclass_scaled_yw{i}
                                                    , if(exc_prev_yw1.next_week_num <> 0 and src.week_num - exc_prev_yw2.next_week_num <= {i} and src.week_num - exc_prev_yw1.next_week_num >= {i}, NULL, avg_item_qty_by_model_scaled_yw{i}) as avg_item_qty_by_model_scaled_yw{i}""".format(
            i=i)

    for i in range(1, last_year_week_features_right + 1):

        DDL_MART_TICKET_FEATURES_BY_STORE_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                            , `week_item_qty_scaled_next_yw{i}` Nullable(Float32)
                                                            , `avg_purchase_price_next_yw{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_class_scaled_next_yw{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_subclass_scaled_next_yw{i}` Nullable(Float32)
                                                            , `avg_item_qty_by_model_scaled_next_yw{i}` Nullable(Float32)""".format(
            i=i)

        INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT + ", week_item_qty_scaled_next_yw{i}, avg_purchase_price_next_yw{i}, avg_item_qty_by_class_scaled_next_yw{i}, avg_item_qty_by_subclass_scaled_next_yw{i}, avg_item_qty_by_model_scaled_next_yw{i}".format(
            i=i)

        INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                        , y.week_item_qty_scaled_next_yw{i}
                                                        , y.avg_purchase_price_next_yw{i}
                                                        , y.avg_item_qty_by_class_scaled_next_yw{i}
                                                        , y.avg_item_qty_by_subclass_scaled_next_yw{i}
                                                        , y.avg_item_qty_by_model_scaled_next_yw{i}""".format(
            i=i)

        INSERT_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                    , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, week_item_qty_scaled_next_yw{i}) as week_item_qty_scaled_next_yw{i}
                                                    , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, avg_purchase_price_next_yw{i}) as avg_purchase_price_next_yw{i}
                                                    , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, avg_item_qty_by_class_scaled_next_yw{i}) as avg_item_qty_by_class_scaled_next_yw{i}
                                                    , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, avg_item_qty_by_subclass_scaled_next_yw{i}) as avg_item_qty_by_subclass_scaled_next_yw{i}
                                                    , if(exc_next_yw1.next_week_num <> 0 and src.week_num - exc_next_yw2.next_week_num >= -{i} and src.week_num - exc_next_yw1.next_week_num <= -{i}, NULL, avg_item_qty_by_model_scaled_next_yw{i}) as avg_item_qty_by_model_scaled_next_yw{i}""".format(
            i=i)

    DDL_MART_TICKET_FEATURES_BY_STORE_SQL = DDL_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                        )
                                                        ENGINE = MergeTree()
                                                        PARTITION BY week_num
                                                        ORDER BY (week_num,
                                                         item, store)
                                                        SETTINGS index_granularity = 8192;
                                                        """

    INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT = INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT + ")"

    INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                    FROM (
                                                        SELECT * 
                                                        FROM {database_name}.{ticket_features_by_store_week_table_name}
                                                        WHERE week_num = {week_num}
                                                    ) w
                                                    LEFT JOIN (
                                                        SELECT * 
                                                        FROM {database_name}.{ticket_features_by_store_year_table_name}
                                                        WHERE week_num = {week_num}
                                                    ) y
                                                    ON w.week_num = y.week_num
                                                        AND w.item = y.item 
                                                        AND w.store = y.store
                                                    """

    INSERT_MART_TICKET_FEATURES_BY_STORE_SQL = INSERT_MART_TICKET_FEATURES_BY_STORE_STATEMENT + INSERT_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                            FROM (""" + INSERT_SUBQUERY_MART_TICKET_FEATURES_BY_STORE_SQL + """
                                                ) src
                                            LEFT JOIN {database_name}.{dict_week_exception_table_name} exc
                                                ON src.week_num = exc.week_num
                                            ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_prev_w1
                                                ON src.year = exc_prev_w1.year AND toUInt32(src.week_num) <= exc_prev_w1.week_num + {current_year_week_features_left}
                                            ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_prev_w2
                                                ON src.year = exc_prev_w2.year AND src.week_num >= exc_prev_w2.week_num
                                            ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_next_w1
                                                ON src.year = exc_next_w1.year AND toInt32(src.week_num) >= exc_next_w1.week_num - {current_year_week_features_right}
                                            ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_next_w2
                                                ON src.year = exc_next_w2.year AND src.week_num <= exc_next_w2.week_num
                                            ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_prev_yw1
                                                ON toUInt64(src.year) = exc_prev_yw1.year + 1 AND toUInt32(src.week_num) <= exc_prev_yw1.next_week_num + {last_year_week_features_left}
                                            ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_prev_yw2
                                                ON toUInt64(src.year) = exc_prev_yw2.year + 1 AND src.week_num >= exc_prev_yw2.next_week_num
                                            ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_next_yw1
                                                ON toUInt64(src.year) = exc_next_yw1.year + 1 AND toInt32(src.week_num) >= exc_next_yw1.next_week_num - {last_year_week_features_right}
                                            ASOF LEFT JOIN {database_name}.{dict_week_exception_table_name} exc_next_yw2
                                                ON toUInt64(src.year) = exc_next_yw2.year + 1 AND src.week_num <= exc_next_yw2.next_week_num
                                            WHERE src.week_num <> exc.week_num 
                                            """

    # BUILD ticket_features_by_store
    logger.info("build {database_name}.{table_name}".format(database_name=database_name,
                                                            table_name=ticket_features_by_store_table_name))
    logger.info(DROP_TABLE_SQL.format(database_name=database_name,
                                      table_name=ticket_features_by_store_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DROP_TABLE_SQL.format(database_name=database_name,
                                                                      table_name=ticket_features_by_store_table_name)])

    logger.info(DDL_MART_TICKET_FEATURES_BY_STORE_SQL.format(database_name=database_name,
                                                             ticket_features_by_store_table_name=ticket_features_by_store_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[
                             DDL_MART_TICKET_FEATURES_BY_STORE_SQL.format(database_name=database_name,
                                                                          ticket_features_by_store_table_name=ticket_features_by_store_table_name)])

    ticket_features_by_store_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                                table_name=ticket_features_by_store_table_name,
                                                                                                                week_cnt=week_cnt)],
                                                             return_result=True,
                                                             logger=logger)[0][0][0]

    logger.info(
        "build {database_name}.{table_name} from {min_week} week to {max_week}".format(database_name=database_name,
                                                                                       table_name=ticket_features_by_store_table_name,
                                                                                       min_week=ticket_features_by_store_min_week,
                                                                                       max_week=max_week))

    for week_num in range(ticket_features_by_store_min_week, max_week + 1):
        logger.info("inserting week_num = {week_num} into {database_name}.{table_name}".format(week_num=week_num,
                                                                                               database_name=database_name,
                                                                                               table_name=ticket_features_by_store_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[
                                 INSERT_MART_TICKET_FEATURES_BY_STORE_SQL.format(database_name=database_name,
                                                                                 ticket_features_by_store_table_name=ticket_features_by_store_table_name,
                                                                                 ticket_features_by_store_week_table_name=ticket_features_by_store_week_table_name,
                                                                                 ticket_features_by_store_year_table_name=ticket_features_by_store_year_table_name,
                                                                                 dict_week_exception_table_name=dict_week_exception_table_name,
                                                                                 current_year_week_features_left=current_year_week_features_left,
                                                                                 current_year_week_features_right=current_year_week_features_right,
                                                                                 last_year_week_features_left=last_year_week_features_left,
                                                                                 last_year_week_features_right=last_year_week_features_right,
                                                                                 week_num=week_num)])

    logger.info("build {database_name}.{table_name} end".format(database_name=database_name,
                                                                table_name=ticket_features_by_store_table_name))


# build_item_qty_to_drop_outlier = PythonOperator(
#     task_id="build_item_qty_to_drop_outlier",
#     python_callable=build_item_qty_to_drop_outlier,
#     provide_context=True,
#     dag=load_tickets_features_dag
# )
#
#
# load_tickets = PythonOperator(
#     task_id="load_tickets",
#     python_callable=load_tickets,
#     provide_context=True,
#     dag=load_tickets_features_dag
# )

load_agg_stock_by_store_op = PythonOperator(
    task_id="load_agg_stock_by_store",
    python_callable=load_agg_stock_by_store,
    op_kwargs={'database_name' : p_database_name,
               'dict_store_table_name' : p_dict_store_table_name,
               'upsampled_table_name' : p_upsampled_table_name,
               'agg_stock_by_store_table_name' : p_agg_stock_by_store_table_name,
               'week_cnt' : p_week_cnt,
               'stock_table_name' : p_stock_table_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_agg_stock_by_week_op = PythonOperator(
    task_id="load_agg_stock_by_week",
    python_callable=load_agg_stock_by_week,
    op_kwargs={'database_name' : p_database_name,
               'agg_stock_by_week_table_name' : p_agg_stock_by_week_table_name,
               'agg_stock_by_store_table_name' : p_agg_stock_by_store_table_name,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_item_attr_op = PythonOperator(
    task_id="load_item_attr",
    python_callable=load_item_attr,
    op_kwargs={'database_name' : p_database_name,
               'item_attr_table_name' : p_item_attr_table_name,
               'subclass_table_name' : p_subclass_table_name,
               'item_master_table_name' : p_item_master_table_name,
               'products_info_table_name' : p_products_info_table_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_agg_tickets_by_store_op = PythonOperator(
    task_id="load_agg_tickets_by_store",
    python_callable=load_agg_tickets_by_store,
    op_kwargs={'database_name' : p_database_name,
               'item_attr_table_name' : p_item_attr_table_name,
               'agg_tickets_by_store_table_name' : p_agg_tickets_by_store_table_name,
               'week_cnt' : p_week_cnt,
               'upsampled_table_name' : p_upsampled_table_name,
               'dict_store_table_name' : p_dict_store_table_name,
               'uda_item_date_view_name' : p_uda_item_date_view_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_avg_tickets_by_week_op = PythonOperator(
    task_id="load_avg_tickets_by_week",
    python_callable=load_avg_tickets_by_week,
    op_kwargs={'database_name' : p_database_name,
               'avg_tickets_by_week_table_name' : p_avg_tickets_by_week_table_name,
               'agg_tickets_by_store_table_name' : p_agg_tickets_by_store_table_name,
               'week_cnt' : p_week_cnt,
               'upsampled_table_name' : p_upsampled_table_name,
               'agg_stock_by_store_table_name' : p_agg_stock_by_store_table_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_sum_tickets_by_week_op = PythonOperator(
    task_id="load_sum_tickets_by_week",
    python_callable=load_sum_tickets_by_week,
    op_kwargs={'database_name' : p_database_name,
               'sum_tickets_by_week_table_name' : p_sum_tickets_by_week_table_name,
               'agg_tickets_by_store_table_name' : p_agg_tickets_by_store_table_name,
               'week_cnt' : p_week_cnt,
               'upsampled_table_name' : p_upsampled_table_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_avg_week_tickets_by_class_op = PythonOperator(
    task_id="load_avg_week_tickets_by_class",
    python_callable=load_avg_week_tickets_by_class,
    op_kwargs={'database_name' : p_database_name,
               'avg_week_tickets_by_class_table_name' : p_avg_week_tickets_by_class_table_name,
               'avg_tickets_by_week_view_name' : p_avg_tickets_by_week_view_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_avg_week_tickets_by_subclass_op = PythonOperator(
    task_id="load_avg_week_tickets_by_subclass",
    python_callable=load_avg_week_tickets_by_subclass,
    op_kwargs={'database_name' : p_database_name,
               'avg_week_tickets_by_subclass_table_name' : p_avg_week_tickets_by_subclass_table_name,
               'avg_tickets_by_week_view_name' : p_avg_tickets_by_week_view_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_avg_week_tickets_by_model_op = PythonOperator(
    task_id="load_avg_week_tickets_by_model",
    python_callable=load_avg_week_tickets_by_model,
    op_kwargs={'database_name' : p_database_name,
               'avg_week_tickets_by_model_table_name' : p_avg_week_tickets_by_model_table_name,
               'avg_tickets_by_week_view_name' : p_avg_tickets_by_week_view_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_agg_tickets_attr_by_week_op = PythonOperator(
    task_id="load_agg_tickets_attr_by_week",
    python_callable=load_agg_tickets_attr_by_week,
    op_kwargs={'database_name' : p_database_name,
               'agg_tickets_attr_by_week_table_name' : p_agg_tickets_attr_by_week_table_name,
               'sum_tickets_by_week_table_name' : p_sum_tickets_by_week_table_name,
               'avg_week_tickets_by_class_view_name' : p_avg_week_tickets_by_class_view_name,
               'avg_week_tickets_by_subclass_view_name' : p_avg_week_tickets_by_subclass_view_name,
               'avg_week_tickets_by_model_view_name' : p_avg_week_tickets_by_model_view_name,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt,
               'agg_tickets_attr_by_week_max_val_table_name' : p_agg_tickets_attr_by_week_max_val_table_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_ticket_features_by_week_op = PythonOperator(
    task_id="load_ticket_features_by_week",
    python_callable=load_ticket_features_by_week,
    op_kwargs={'database_name' : p_database_name,
               'ticket_features_by_week_table_name' : p_ticket_features_by_week_table_name,
               'agg_tickets_attr_by_week_view_name' : p_agg_tickets_attr_by_week_view_name,
               'current_year_week_features_left' : p_current_year_week_features_left,
               'current_year_week_features_right' : p_current_year_week_features_right,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_ticket_features_by_year_op = PythonOperator(
    task_id="load_ticket_features_by_year",
    python_callable=load_ticket_features_by_year,
    op_kwargs={'database_name' : p_database_name,
               'ticket_features_by_year_table_name' : p_ticket_features_by_year_table_name,
               'agg_tickets_attr_by_week_view_name' : p_agg_tickets_attr_by_week_view_name,
               'calendar_table_name' : p_calendar_table_name,
               'last_year_week_features_left' : p_last_year_week_features_left,
               'last_year_week_features_right' : p_last_year_week_features_right,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_ticket_features_op = PythonOperator(
    task_id="load_ticket_features",
    python_callable=load_ticket_features,
    op_kwargs={'database_name' : p_database_name,
               'ticket_features_table_name' : p_ticket_features_table_name,
               'ticket_features_by_week_table_name' : p_ticket_features_by_week_table_name,
               'ticket_features_by_year_table_name' : p_ticket_features_by_year_table_name,
               'current_year_week_features_left' : p_current_year_week_features_left,
               'current_year_week_features_right' : p_current_year_week_features_right,
               'last_year_week_features_left' : p_last_year_week_features_left,
               'last_year_week_features_right' : p_last_year_week_features_right,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt,
               'dict_week_exception_table_name' : p_dict_week_exception_table_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_agg_tickets_attr_by_store_op = PythonOperator(
    task_id="load_agg_tickets_attr_by_store",
    python_callable=load_agg_tickets_attr_by_store,
    op_kwargs={'database_name' : p_database_name,
               'agg_tickets_attr_by_store_table_name' : p_agg_tickets_attr_by_store_table_name,
               'agg_tickets_by_store_table_name' : p_agg_tickets_by_store_table_name,
               'avg_week_tickets_by_class_view_name' : p_avg_week_tickets_by_class_view_name,
               'avg_week_tickets_by_subclass_view_name' : p_avg_week_tickets_by_subclass_view_name,
               'avg_week_tickets_by_model_view_name' : p_avg_week_tickets_by_model_view_name,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt,
               'agg_tickets_attr_by_store_max_val_table_name' : p_agg_tickets_attr_by_store_max_val_table_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_ticket_features_by_store_week_op = PythonOperator(
    task_id="load_ticket_features_by_store_week",
    python_callable=load_ticket_features_by_store_week,
    op_kwargs={'database_name' : p_database_name,
               'ticket_features_by_store_week_table_name' : p_ticket_features_by_store_week_table_name,
               'agg_tickets_attr_by_store_view_name' : p_agg_tickets_attr_by_store_view_name,
               'current_year_week_features_left' : p_current_year_week_features_left,
               'current_year_week_features_right' : p_current_year_week_features_right,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

load_ticket_features_by_store_year_op = PythonOperator(
    task_id="load_ticket_features_by_store_year",
    python_callable=load_ticket_features_by_store_year,
    op_kwargs={'database_name' : p_database_name,
               'ticket_features_by_store_year_table_name' : p_ticket_features_by_store_year_table_name,
               'agg_tickets_attr_by_store_view_name' : p_agg_tickets_attr_by_store_view_name,
               'calendar_table_name' : p_calendar_table_name,
               'last_year_week_features_left' : p_last_year_week_features_left,
               'last_year_week_features_right' : p_last_year_week_features_right,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt
               },
    provide_context=True,
    dag=load_tickets_features_dag
)

ticket_features_by_store_op = PythonOperator(
    task_id="load_ticket_features_by_store",
    python_callable=load_ticket_features_by_store,
    op_kwargs={'database_name' : p_database_name,
               'ticket_features_by_store_table_name' : p_ticket_features_by_store_table_name,
               'ticket_features_by_store_week_table_name' : p_ticket_features_by_store_week_table_name,
               'ticket_features_by_store_year_table_name' : p_ticket_features_by_store_year_table_name,
               'current_year_week_features_left' : p_current_year_week_features_left,
               'current_year_week_features_right' : p_current_year_week_features_right,
               'last_year_week_features_left' : p_last_year_week_features_left,
               'last_year_week_features_right' : p_last_year_week_features_right,
               'upsampled_table_name' : p_upsampled_table_name,
               'week_cnt' : p_week_cnt,
               'dict_week_exception_table_name' : p_dict_week_exception_table_name
               },
    provide_context=True,
    dag=load_tickets_features_dag
)



# build_item_qty_to_drop_outlier >> load_tickets >> [load_agg_stock_by_store_op, load_item_attr_op]

load_agg_stock_by_store_op >> [load_agg_stock_by_week_op, load_avg_tickets_by_week_op]
load_item_attr_op >> load_agg_tickets_by_store_op >> [load_avg_tickets_by_week_op, load_sum_tickets_by_week_op]
load_avg_tickets_by_week_op >> [load_avg_week_tickets_by_class_op, load_avg_week_tickets_by_subclass_op, load_avg_week_tickets_by_model_op]
load_sum_tickets_by_week_op >> load_agg_tickets_attr_by_week_op
load_avg_week_tickets_by_class_op >> [load_agg_tickets_attr_by_week_op, load_agg_tickets_attr_by_store_op]
load_avg_week_tickets_by_subclass_op >> [load_agg_tickets_attr_by_week_op, load_agg_tickets_attr_by_store_op]
load_avg_week_tickets_by_model_op >> [load_agg_tickets_attr_by_week_op, load_agg_tickets_attr_by_store_op]
load_agg_tickets_attr_by_week_op >> [load_ticket_features_by_week_op, load_ticket_features_by_year_op] >> load_ticket_features_op
load_agg_tickets_attr_by_store_op >> [load_ticket_features_by_store_week_op, load_ticket_features_by_store_year_op] >> ticket_features_by_store_op
