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
    schedule_interval="30 6 * * 2",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


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
                            WHERE 
                                store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
                            GROUP BY item
                         ) ot
                         FULL OUTER JOIN (
                            SELECT 
                                toInt32OrZero(item) AS item, 
                                max(item_qty) AS item_qty_max_raw_online, 
                                quantile(0.95)(item_qty) AS item_qty_95_raw_online, 
                                count() AS ticket_qty_online
                            FROM {database_name}.view_mart_online_orders_365d vmood 
                            WHERE 
                                store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
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
                                    WHERE store in (2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 162, 171, 176)
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


def load_ticket_features(ds, **kwargs):

    from sf_api.run_clickhouse import run_clickhouse_query

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    week_cnt = 12  # кол-во расчетных недель
    smooth_interval = 5  # окно, за которое мы сглаживаем продажи
    stores = [2, 3, 4, 5, 6, 20, 22, 26, 32, 35, 40, 43, 45, 49, 51, 56, 62, 65, 86, 114, 117, 122, 143, 153, 171, 176, 162]
    database_name = 'salesfs'
    upsampled_table_name = 'work_tickets_agg_day_store_item_upsampled'
    agg_tickets_by_week_table_name = 'tmp_agg_tickets_by_week'
    calendar_table_name = 'work_calendar_for_tickets'
    ticket_features_by_week_table_name = 'work_ticket_features_by_week'
    ticket_features_by_year_table_name = 'work_ticket_features_by_year'
    ticket_features_table_name = 'work_ticket_features'


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

    # tmp_agg_tickets_by_week -- недельный агрегат, временный
    # надо пересчитывать каждый раз на 6 недель назад
    INSERT_TMP_AGG_TICKETS_BY_WEEK_SQL = """
        INSERT INTO {database_name}.{agg_tickets_by_week_table_name}
        SELECT t1."year"
               , t1.week
               , t1.week_num
               , t1.store
               , t1.item
               , t1.week_item_qty
               , t1.avg_item_qty
               , t1.purchase_price
               , avg(t2.item_qty) as smooth_item_qty
        FROM 
            (
              SELECT
                     toYear(toMonday(tads.sell_date)) as "year"
                   , toRelativeWeekNum(toMonday(tads.sell_date)) as week_num
                   , toRelativeWeekNum(toMonday(tads.sell_date)) - toRelativeWeekNum(toDateTime(toString(toYear(toMonday(tads.sell_date))-1) || '-12-31 00:00:00')) as week
                   , tads.store
                   , tads.item 
                   , sum(item_qty) as week_item_qty
                   , avg(item_qty) as avg_item_qty
                   , avg(purchase_price) as purchase_price
              FROM {database_name}.{upsampled_table_name} tads
              WHERE week_num = {week_num}
                AND tads.store = {store_id} 
              GROUP BY toYear(toMonday(tads.sell_date)) as "year"
                     , toRelativeWeekNum(toMonday(tads.sell_date)) - toRelativeWeekNum(toDateTime(toString(toYear(toMonday(tads.sell_date))-1) || '-12-31 00:00:00')) as week
                     , toRelativeWeekNum(toMonday(tads.sell_date)) as week_num
                     , tads.store
                     , tads.item
            ) t1
            -- джойн для рассчета сглаживания
            LEFT JOIN 
            (
              SELECT
                     toRelativeWeekNum(toMonday(tads.sell_date)) as week_num
                   , tads.store
                   , tads.item 
                   , sum(tads.item_qty) as item_qty
              FROM {database_name}.{upsampled_table_name} tads
              WHERE week_num between {week_num} - {smooth_interval}/2 AND {week_num} + {smooth_interval}/2
                AND tads.store = {store_id}
              GROUP BY toRelativeWeekNum(toMonday(tads.sell_date)) as week_num
                     , tads.store
                     , tads.item 
            ) t2
            ON t1.store = t2.store
            AND t1.item = t2.item
        GROUP BY t1."year"
               , t1.week
               , t1.week_num
               , t1.store
               , t1.item
               , t1.week_item_qty
               , t1.avg_item_qty
               , t1.purchase_price
        """

    TRUNCATE_SQL = """TRUNCATE TABLE {database_name}.{table_name}"""

    DROP_PART_SQL = """ALTER TABLE {database_name}.{table_name} DROP PARTITION '{partition_field}'"""

    # -- таблица для расчета годовых фичей
    # неделя в предыдущем году относительно номера текущей недели
    INSERT_WORK_CALENDAR_FOR_TICKETS_SQL = """
            INSERT INTO {database_name}.{calendar_table_name}
            SELECT
                t1.year
                , t1.week
                , t1.week_num
                , t1.week_num - t2.week_diff as prev_week_num
            FROM(
                SELECT distinct year, week, week_num 
                FROM {database_name}.{agg_tickets_by_week_table_name}
                ) t1
            LEFT JOIN (
                SELECT year, toRelativeWeekNum(toMonday(toDate(toString(year)||'-12-31')))-min(week_num)+1 as week_diff
                FROM {database_name}.{agg_tickets_by_week_table_name}
                GROUP BY year
            ) t2
                ON t1.year = t2.year
            """

    # -- work_ticket_features_by_week -- таблица с предподготовленными недельными фичами
    # высчитываются параметры из прошлого и из будущего (прошлый год +- 6 недель) в текущем году
    INSERT_WORK_TICKET_FEATURES_BY_WEEK_SQL = """
            INSERT INTO {database_name}.{ticket_features_by_week_table_name}
                SELECT tatbw.`year`
                    , tatbw.week
                    , tatbw.week_num
                    , tatbw.store
                    , tatbw.item
                    , tatbw.item_qty
                    , tatbw.smooth_item_qty
                    , tatbw.avg_purchase_price
                    , sum(if(toUInt16(tatbw.week_num - 1) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_prev_w1
                    , sum(if(toUInt16(tatbw.week_num - 1) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_prev_w1
                    , sum(if(toUInt16(tatbw.week_num - 1) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_prev_w1
                    , sum(if(toUInt16(tatbw.week_num - 2) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_prev_w2
                    , sum(if(toUInt16(tatbw.week_num - 2) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_prev_w2
                    , sum(if(toUInt16(tatbw.week_num - 2) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_prev_w2
                    , sum(if(toUInt16(tatbw.week_num - 3) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_prev_w3
                    , sum(if(toUInt16(tatbw.week_num - 3) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_prev_w3
                    , sum(if(toUInt16(tatbw.week_num - 3) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_prev_w3
                    , sum(if(toUInt16(tatbw.week_num - 4) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_prev_w4
                    , sum(if(toUInt16(tatbw.week_num - 4) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_prev_w4
                    , sum(if(toUInt16(tatbw.week_num - 4) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_prev_w4
                    , sum(if(toUInt16(tatbw.week_num - 5) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_prev_w5
                    , sum(if(toUInt16(tatbw.week_num - 5) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_prev_w5
                    , sum(if(toUInt16(tatbw.week_num - 5) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_prev_w5
                    , sum(if(toUInt16(tatbw.week_num - 6) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_prev_w6
                    , sum(if(toUInt16(tatbw.week_num - 6) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_prev_w6
                    , sum(if(toUInt16(tatbw.week_num - 6) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_prev_w6
                    , sum(if(toUInt16(tatbw.week_num + 1) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_next_w1
                    , sum(if(toUInt16(tatbw.week_num + 1) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_w1
                    , sum(if(toUInt16(tatbw.week_num + 1) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_w1
                    , sum(if(toUInt16(tatbw.week_num + 2) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_next_w2
                    , sum(if(toUInt16(tatbw.week_num + 2) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_w2
                    , sum(if(toUInt16(tatbw.week_num + 2) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_w2
                    , sum(if(toUInt16(tatbw.week_num + 3) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_next_w3
                    , sum(if(toUInt16(tatbw.week_num + 3) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_w3
                    , sum(if(toUInt16(tatbw.week_num + 3) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_w3
                    , sum(if(toUInt16(tatbw.week_num + 4) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_next_w4
                    , sum(if(toUInt16(tatbw.week_num + 4) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_w4
                    , sum(if(toUInt16(tatbw.week_num + 4) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_w4
                    , sum(if(toUInt16(tatbw.week_num + 5) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_next_w5
                    , sum(if(toUInt16(tatbw.week_num + 5) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_w5
                    , sum(if(toUInt16(tatbw.week_num + 5) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_w5
                    , sum(if(toUInt16(tatbw.week_num + 6) = prev_tatbw.week_num, prev_tatbw.item_qty, NULL)) as item_qty_next_w6
                    , sum(if(toUInt16(tatbw.week_num + 6) = prev_tatbw.week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_w6
                    , sum(if(toUInt16(tatbw.week_num + 6) = prev_tatbw.week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_w6
                FROM (
                    SELECT `year`
                        , week
                        , week_num
                        , store
                        , item
                        , item_qty
                        , smooth_item_qty
                        , avg_purchase_price
                    FROM {database_name}.{agg_tickets_by_week_table_name} tatbw 
                    WHERE week_num = {week_num}) tatbw
                LEFT JOIN 
                   (
                    SELECT week_num
                        , store
                        , item
                        , item_qty
                        , smooth_item_qty
                        , avg_purchase_price
                        , 1 as prev
                    FROM {database_name}.{agg_tickets_by_week_table_name} tatbw 
                    WHERE week_num between ({week_num} - 6) AND ({week_num} + 6)
                   ) prev_tatbw
                ON tatbw.item = prev_tatbw.item
                    AND tatbw.store = prev_tatbw.store
                GROUP BY tatbw.`year`
                , tatbw.week
                , tatbw.week_num
                , tatbw.store
                , tatbw.item
                , tatbw.item_qty
                , tatbw.smooth_item_qty
                , tatbw.avg_purchase_price
            """

    # -- годовые фичи
    # высчитываются параметры из прошлого и из будущего (прошлый год +- 12 недель) в прошлом году
    INSERT_WORK_TICKET_FEATURES_BY_YEAR_SQL = """
            INSERT INTO {database_name}.{ticket_features_by_year_table_name}
                SELECT
                    t1.`year`	
                    , t1.week
                    , t1.week_num
                    , t1.store
                    , t1.item
                    , sum(if(prev_tatbw.week_num = t1.prev_week_num, prev_tatbw.item_qty, NULL)) as item_qty_y
                    , sum(if(prev_tatbw.week_num = t1.prev_week_num, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_y
                    , sum(if(prev_tatbw.week_num = t1.prev_week_num, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_y
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 1, prev_tatbw.item_qty, NULL)) as item_qty_yw1
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 1, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_yw1
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 1, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_yw1
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 2, prev_tatbw.item_qty, NULL)) as item_qty_yw2
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 2, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_yw2
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 2, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_yw2
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 3, prev_tatbw.item_qty, NULL)) as item_qty_yw3
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 3, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_yw3
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 3, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_yw3
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 4, prev_tatbw.item_qty, NULL)) as item_qty_yw4
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 4, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_yw4
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 4, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_yw4
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 5, prev_tatbw.item_qty, NULL)) as item_qty_yw5
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 5, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_yw5
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 5, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_yw5
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 6, prev_tatbw.item_qty, NULL)) as item_qty_yw6
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 6, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_yw6
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num - 6, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_yw6
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 1, prev_tatbw.item_qty, NULL)) as item_qty_next_yw1
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 1, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw1
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 1, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw1
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 2, prev_tatbw.item_qty, NULL)) as item_qty_next_yw2
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 2, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw2
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 2, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw2
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 3, prev_tatbw.item_qty, NULL)) as item_qty_next_yw3
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 3, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw3
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 3, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw3
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 4, prev_tatbw.item_qty, NULL)) as item_qty_next_yw4
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 4, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw4
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 4, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw4
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 5, prev_tatbw.item_qty, NULL)) as item_qty_next_yw5
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 5, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw5
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 5, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw5
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 6, prev_tatbw.item_qty, NULL)) as item_qty_next_yw6
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 6, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw6
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 6, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw6
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 7, prev_tatbw.item_qty, NULL)) as item_qty_next_yw7
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 7, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw7
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 7, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw7
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 8, prev_tatbw.item_qty, NULL)) as item_qty_next_yw8
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 8, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw8
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 8, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw8
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 9, prev_tatbw.item_qty, NULL)) as item_qty_next_yw9
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 9, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw9
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 9, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw9
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 10, prev_tatbw.item_qty, NULL)) as item_qty_next_yw10
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 10, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw10
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 10, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw10
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 11, prev_tatbw.item_qty, NULL)) as item_qty_next_yw11
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 11, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw11
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 11, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw11
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 12, prev_tatbw.item_qty, NULL)) as item_qty_next_yw12
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 12, prev_tatbw.smooth_item_qty, NULL)) as smooth_item_qty_next_yw12
                    , sum(if (prev_tatbw.week_num = t1.prev_week_num + 12, prev_tatbw.avg_purchase_price, NULL)) as avg_purchase_price_next_yw12
                FROM ( 
                    SELECT 
                        tatbw.`year`	
                        , tatbw.week
                        , tatbw.week_num
                        , tatbw.store
                        , tatbw.item
                        , tatbw.item_qty
                        , tatbw.smooth_item_qty
                        , tatbw.avg_item_qty
                        , tatbw.avg_purchase_price
                        , wcft.prev_week_num
                    FROM(
                        SELECT 
                            `year`
                            , week
                            , week_num
                            , store
                            , item
                            , item_qty
                            , smooth_item_qty
                            , avg_item_qty
                            , avg_purchase_price
                        FROM {database_name}.{agg_tickets_by_week_table_name} 
                        WHERE week_num = {week_num}
                    ) tatbw
                    INNER JOIN {database_name}.{calendar_table_name} wcft
                        ON tatbw.week_num = wcft.week_num 
                    ) t1
                INNER JOIN 
                    (SELECT
                        agg.week_num
                        , agg.store
                        , agg.item
                        , agg.item_qty
                        , agg.smooth_item_qty
                        , agg.avg_item_qty
                        , agg.avg_purchase_price
                     FROM {database_name}.{agg_tickets_by_week_table_name} agg
                     WHERE agg.week_num between (SELECT prev_week_num - 6 FROM {database_name}.{calendar_table_name} WHERE week_num = {week_num}) 
                                            AND (SELECT prev_week_num + 12 FROM {database_name}.{calendar_table_name} WHERE week_num = {week_num}) 
                )prev_tatbw
                ON t1.store = prev_tatbw.store
                AND t1.item = prev_tatbw.item
                GROUP BY t1.`year`	
                    , t1.week
                    , t1.week_num
                    , t1.store
                    , t1.item
            """

    ##################

    # -- итоготовая таблица
    # текущая неделя + джойн текущего года +- 6 недель + джойн предыдущего года +- 12 недель
    INSERT_WORK_TICKET_FEATURES_SQL = """
            INSERT INTO {database_name}.{ticket_features_table_name}
                SELECT bw.`year`
                        , bw.week
                        , bw.week_num 
                        , bw.item 
                        , sum(bw.item_qty) as item_qty
                        , sum(bw.smooth_item_qty) as smooth_item_qty
                        , avg(bw.avg_purchase_price) as avg_purchase_price
                        , sum(bw.item_qty_prev_w1) as item_qty_prev_w1
                        , sum(bw.smooth_item_qty_prev_w1) as smooth_item_qty_prev_w1
                        , avg(bw.avg_purchase_price_prev_w1) as avg_purchase_price_prev_w1
                        , sum(bw.item_qty_prev_w2) as item_qty_prev_w2
                        , sum(bw.smooth_item_qty_prev_w2) as smooth_item_qty_prev_w2
                        , avg(bw.avg_purchase_price_prev_w2) as avg_purchase_price_prev_w2
                        , sum(bw.item_qty_prev_w3) as item_qty_prev_w3
                        , sum(bw.smooth_item_qty_prev_w3) as smooth_item_qty_prev_w3
                        , avg(bw.avg_purchase_price_prev_w3) as avg_purchase_price_prev_w3
                        , sum(bw.item_qty_prev_w4) as item_qty_prev_w4
                        , sum(bw.smooth_item_qty_prev_w4) as smooth_item_qty_prev_w4
                        , avg(bw.avg_purchase_price_prev_w4) as avg_purchase_price_prev_w4
                        , sum(bw.item_qty_prev_w5) as item_qty_prev_w5
                        , sum(bw.smooth_item_qty_prev_w5) as smooth_item_qty_prev_w5
                        , avg(bw.avg_purchase_price_prev_w5) as avg_purchase_price_prev_w5
                        , sum(bw.item_qty_prev_w6) as item_qty_prev_w6
                        , sum(bw.smooth_item_qty_prev_w6) as smooth_item_qty_prev_w6
                        , avg(bw.avg_purchase_price_prev_w6) as avg_purchase_price_prev_w6
                        , sum(bw.item_qty_next_w1) as item_qty_next_w1
                        , sum(bw.smooth_item_qty_next_w1) as smooth_item_qty_next_w1
                        , avg(bw.avg_purchase_price_next_w1) as avg_purchase_price_next_w1
                        , sum(bw.item_qty_next_w2) as item_qty_next_w2
                        , sum(bw.smooth_item_qty_next_w2) as smooth_item_qty_next_w2
                        , avg(bw.avg_purchase_price_next_w2) as avg_purchase_price_next_w2
                        , sum(bw.item_qty_next_w3) as item_qty_next_w3
                        , sum(bw.smooth_item_qty_next_w3) as smooth_item_qty_next_w3
                        , avg(bw.avg_purchase_price_next_w3) as avg_purchase_price_next_w3
                        , sum(bw.item_qty_next_w4) as item_qty_next_w4
                        , sum(bw.smooth_item_qty_next_w4) as smooth_item_qty_next_w4
                        , avg(bw.avg_purchase_price_next_w4) as avg_purchase_price_next_w4
                        , sum(bw.item_qty_next_w5) as item_qty_next_w5
                        , sum(bw.smooth_item_qty_next_w5) as smooth_item_qty_next_w5
                        , avg(bw.avg_purchase_price_next_w5) as avg_purchase_price_next_w5
                        , sum(bw.item_qty_next_w6) as item_qty_next_w6
                        , sum(bw.smooth_item_qty_next_w6) as smooth_item_qty_next_w6
                        , avg(bw.avg_purchase_price_next_w6) as avg_purchase_price_next_w6
                        , sum(bya.tem_qty_y) as tem_qty_y
                        , sum(bya.smooth_item_qty_y) as smooth_item_qty_y
                        , avg(bya.avg_purchase_price_y) as avg_purchase_price_y
                        , sum(bya.item_qty_yw1) as item_qty_yw1
                        , sum(bya.smooth_item_qty_yw1) as smooth_item_qty_yw1
                        , avg(bya.avg_purchase_price_yw1) as avg_purchase_price_yw1
                        , sum(bya.item_qty_yw2) as item_qty_yw2
                        , sum(bya.smooth_item_qty_yw2) as smooth_item_qty_yw2
                        , avg(bya.avg_purchase_price_yw2) as avg_purchase_price_yw2
                        , sum(bya.item_qty_yw3) as item_qty_yw3
                        , sum(bya.smooth_item_qty_yw3) as smooth_item_qty_yw3
                        , avg(bya.avg_purchase_price_yw3) as avg_purchase_price_yw3
                        , sum(bya.item_qty_yw4) as item_qty_yw4
                        , sum(bya.smooth_item_qty_yw4) as smooth_item_qty_yw4
                        , avg(bya.avg_purchase_price_yw4) as avg_purchase_price_yw4
                        , sum(bya.item_qty_yw5) as item_qty_yw5
                        , sum(bya.smooth_item_qty_yw5) as smooth_item_qty_yw5
                        , avg(bya.avg_purchase_price_yw5) as avg_purchase_price_yw5
                        , sum(bya.item_qty_yw6) as item_qty_yw6
                        , sum(bya.smooth_item_qty_yw6) as smooth_item_qty_yw6
                        , avg(bya.avg_purchase_price_yw6) as avg_purchase_price_yw6
                        , sum(bya.item_qty_next_yw1) as item_qty_next_yw1
                        , sum(bya.smooth_item_qty_next_yw1) as smooth_item_qty_next_yw1
                        , avg(bya.avg_purchase_price_next_yw1) as avg_purchase_price_next_yw1
                        , sum(bya.item_qty_next_yw2) as item_qty_next_yw2
                        , sum(bya.smooth_item_qty_next_yw2) as smooth_item_qty_next_yw2
                        , avg(bya.avg_purchase_price_next_yw2) as avg_purchase_price_next_yw2
                        , sum(bya.item_qty_next_yw3) as item_qty_next_yw3
                        , sum(bya.smooth_item_qty_next_yw3) as smooth_item_qty_next_yw3
                        , avg(bya.avg_purchase_price_next_yw3) as avg_purchase_price_next_yw3
                        , sum(bya.item_qty_next_yw4) as item_qty_next_yw4
                        , sum(bya.smooth_item_qty_next_yw4) as smooth_item_qty_next_yw4
                        , avg(bya.avg_purchase_price_next_yw4) as avg_purchase_price_next_yw4
                        , sum(bya.item_qty_next_yw5) as item_qty_next_yw5
                        , sum(bya.smooth_item_qty_next_yw5) as smooth_item_qty_next_yw5
                        , avg(bya.avg_purchase_price_next_yw5) as avg_purchase_price_next_yw5
                        , sum(bya.item_qty_next_yw6) as item_qty_next_yw6
                        , sum(bya.smooth_item_qty_next_yw6) as smooth_item_qty_next_yw6
                        , avg(bya.avg_purchase_price_next_yw6) as avg_purchase_price_next_yw6
                        , sum(bya.item_qty_next_yw7) as item_qty_next_yw7
                        , sum(bya.smooth_item_qty_next_yw7) as smooth_item_qty_next_yw7
                        , avg(bya.avg_purchase_price_next_yw7) as avg_purchase_price_next_yw7
                        , sum(bya.item_qty_next_yw8) as item_qty_next_yw8
                        , sum(bya.smooth_item_qty_next_yw8) as smooth_item_qty_next_yw8
                        , avg(bya.avg_purchase_price_next_yw8) as avg_purchase_price_next_yw8
                        , sum(bya.item_qty_next_yw9) as item_qty_next_yw9
                        , sum(bya.smooth_item_qty_next_yw9) as smooth_item_qty_next_yw9
                        , avg(bya.avg_purchase_price_next_yw9) as avg_purchase_price_next_yw9
                        , sum(bya.item_qty_next_yw10) as item_qty_next_yw10
                        , sum(bya.smooth_item_qty_next_yw10) as smooth_item_qty_next_yw10
                        , avg(bya.avg_purchase_price_next_yw10) as avg_purchase_price_next_yw10
                        , sum(bya.item_qty_next_yw11) as item_qty_next_yw11
                        , sum(bya.smooth_item_qty_next_yw11) as smooth_item_qty_next_yw11
                        , avg(bya.avg_purchase_price_next_yw11) as avg_purchase_price_next_yw11
                        , sum(bya.item_qty_next_yw12) as item_qty_next_yw12
                        , sum(bya.smooth_item_qty_next_yw12) as smooth_item_qty_next_yw12
                        , avg(bya.avg_purchase_price_next_yw12) as avg_purchase_price_next_yw12
                FROM (SELECT * FROM {database_name}.{ticket_features_by_week_table_name} WHERE week_num = {week_num}) bw
                    LEFT JOIN (SELECT * FROM {database_name}.{ticket_features_by_year_table_name} WHERE week_num = {week_num}) bya
                        ON bw.week_num = bya.week_num
                        AND bw.store = bya.store
                        AND bw.item = bya.item 
                GROUP BY bw.`year`
                        , bw.week
                        , bw.week_num 
                        , bw.item
            """

    logger.info("{current_datetime}, start".format(current_datetime=datetime.now()))

    agg_tickets_by_week_min_week = run_clickhouse_query(clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                           table_name=agg_tickets_by_week_table_name,
                                                                                                           week_cnt=week_cnt)],
                                                        return_result=True,
                                                        logger=logger)[0][0][0]

    logger.info("{current_datetime}, build {database_name}.{table_name} from {min_week} week to {max_week}".format(current_datetime=datetime.now(),
                                                                                                                   database_name=database_name,
                                                                                                                   table_name=agg_tickets_by_week_table_name,
                                                                                                                   min_week=agg_tickets_by_week_min_week,
                                                                                                                   max_week=max_week))

    for week_num in range(agg_tickets_by_week_min_week, max_week + 1):
        logger.info(DROP_PART_SQL.format(database_name=database_name,
                                         table_name=agg_tickets_by_week_table_name,
                                         partition_field=week_num))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=agg_tickets_by_week_table_name,
                                                                         partition_field=week_num)])

        logger.info("inserting week_num = {week_num} into {database_name}.{agg_tickets_by_week_table_name}".format(week_num=week_num,
                                                                                                                   database_name=database_name,
                                                                                                                   agg_tickets_by_week_table_name=agg_tickets_by_week_table_name))
        for store in stores:
            run_clickhouse_query(logger=logger,
                                 clickhouse_query_list=[INSERT_TMP_AGG_TICKETS_BY_WEEK_SQL.format(database_name=database_name,
                                                                                                  agg_tickets_by_week_table_name=agg_tickets_by_week_table_name,
                                                                                                  upsampled_table_name=upsampled_table_name,
                                                                                                  week_num=week_num,
                                                                                                  store_id=store,
                                                                                                  smooth_interval=smooth_interval)])

    logger.info("{current_datetime}, build {database_name}.{agg_tickets_by_week_table_name} end".format(current_datetime=datetime.now(),
                                                                                                        database_name=database_name,
                                                                                                        agg_tickets_by_week_table_name=agg_tickets_by_week_table_name))

    #####################

    logger.info("{current_datetime}, build {database_name}.{calendar_table_name}".format(current_datetime=datetime.now(),
                                                                                         database_name=database_name,
                                                                                         calendar_table_name=calendar_table_name))


    logger.info(TRUNCATE_SQL.format(database_name=database_name,
                                    table_name=calendar_table_name))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=calendar_table_name),
                                                INSERT_WORK_CALENDAR_FOR_TICKETS_SQL.format(database_name=database_name,
                                                                                            calendar_table_name=calendar_table_name,
                                                                                            agg_tickets_by_week_table_name=agg_tickets_by_week_table_name)
                                                ])

    logger.info("{current_datetime}, build {database_name}.{calendar_table_name} end".format(current_datetime=datetime.now(),
                                                                                             database_name=database_name,
                                                                                             calendar_table_name=calendar_table_name))

    ##################



    ticket_features_by_week_min_week = run_clickhouse_query(logger=logger,
                                                            return_result=True,
                                                            clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                               table_name=ticket_features_by_week_table_name,
                                                                                                               week_cnt=week_cnt)
                                                                                   ])[0][0][0]

    logger.info("{current_datetime}, build {database_name}.{table_name} from {min_week} week to {max_week}".format(current_datetime=datetime.now(),
                                                                                                                   database_name=database_name,
                                                                                                                   table_name=ticket_features_by_week_table_name,
                                                                                                                   min_week=ticket_features_by_week_min_week,
                                                                                                                   max_week=max_week))

    for week_num in range(ticket_features_by_week_min_week, max_week + 1):
        logger.info(DROP_PART_SQL.format(database_name=database_name,
                                         table_name=ticket_features_by_week_table_name,
                                         partition_field=week_num))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=ticket_features_by_week_table_name,
                                                                         partition_field=week_num)
                                                    ])
        logger.info("inserting week_num = {week_num} into {database_name}.{ticket_features_by_week_table_name}".format(database_name=database_name,
                                                                                                                       ticket_features_by_week_table_name=ticket_features_by_week_table_name,
                                                                                                                       week_num=week_num))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[INSERT_WORK_TICKET_FEATURES_BY_WEEK_SQL.format(database_name=database_name,
                                                                                                   ticket_features_by_week_table_name=ticket_features_by_week_table_name,
                                                                                                   agg_tickets_by_week_table_name=agg_tickets_by_week_table_name,
                                                                                                   week_num=week_num)
                                                    ])

    logger.info("{current_datetime}, build {database_name}.{ticket_features_by_week_table_name} end".format(current_datetime=datetime.now(),
                                                                                                            database_name=database_name,
                                                                                                            ticket_features_by_week_table_name=ticket_features_by_week_table_name))

    ##################



    ticket_features_by_year_min_week = run_clickhouse_query(return_result=True,
                                                            logger=logger,
                                                            clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                               table_name=ticket_features_by_year_table_name,
                                                                                                               week_cnt=week_cnt)
                                                                                   ])[0][0][0]

    logger.info("{current_datetime}, build {database_name}.{table_name} from {min_week} week to {max_week}".format(current_datetime=datetime.now(),
                                                                                                                   database_name=database_name,
                                                                                                                   table_name=ticket_features_by_year_table_name,
                                                                                                                   min_week=ticket_features_by_year_min_week,
                                                                                                                   max_week=max_week))

    for week_num in range(ticket_features_by_year_min_week, max_week + 1):
        logger.info(DROP_PART_SQL.format(database_name=database_name,
                                         table_name=ticket_features_by_year_table_name,
                                         partition_field=week_num))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=ticket_features_by_year_table_name,
                                                                         partition_field=week_num)
                                                    ])
        logger.info("inserting week_num = {week_num} into {database_name}.{ticket_features_by_year_table_name}".format(week_num=week_num,
                                                                                                                       database_name=database_name,
                                                                                                                       ticket_features_by_year_table_name=ticket_features_by_year_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[INSERT_WORK_TICKET_FEATURES_BY_YEAR_SQL.format(database_name=database_name,
                                                                                                   ticket_features_by_year_table_name=ticket_features_by_year_table_name,
                                                                                                   agg_tickets_by_week_table_name=agg_tickets_by_week_table_name,
                                                                                                   calendar_table_name=calendar_table_name,
                                                                                                   week_num=week_num)
                                                    ])

    logger.info("{current_datetime}, build {database_name}.{ticket_features_by_year_table_name} end".format(current_datetime=datetime.now(),
                                                                                                            database_name=database_name,
                                                                                                            ticket_features_by_year_table_name=ticket_features_by_year_table_name))

    ticket_features_min_week = run_clickhouse_query(return_result=True,
                                                    logger=logger,
                                                    clickhouse_query_list=[GET_MIN_WEEK_NUM_SQL.format(database_name=database_name,
                                                                                                       table_name=ticket_features_table_name,
                                                                                                       week_cnt=week_cnt)
                                                                           ])[0][0][0]

    logger.info("{current_datetime}, build {database_name}.{table_name} from {min_week} week to {max_week}".format(current_datetime=datetime.now(),
                                                                                                                   database_name=database_name,
                                                                                                                   table_name=ticket_features_table_name,
                                                                                                                   min_week=ticket_features_min_week,
                                                                                                                   max_week=max_week))

    for week_num in range(ticket_features_min_week, max_week + 1):
        logger.info(DROP_PART_SQL.format(database_name=database_name,
                                         table_name=ticket_features_table_name,
                                         partition_field=week_num))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[DROP_PART_SQL.format(database_name=database_name,
                                                                         table_name=ticket_features_table_name,
                                                                         partition_field=week_num)
                                                    ])
        logger.info("inserting week_num = {week_num} into {ticket_features_table_name}".format(week_num=week_num,
                                                                                               ticket_features_table_name=ticket_features_table_name))
        run_clickhouse_query(logger=logger,
                             clickhouse_query_list=[INSERT_WORK_TICKET_FEATURES_SQL.format(database_name=database_name,
                                                                                           ticket_features_table_name=ticket_features_table_name,
                                                                                           ticket_features_by_week_table_name=ticket_features_by_week_table_name,
                                                                                           ticket_features_by_year_table_name=ticket_features_by_year_table_name,
                                                                                           week_num=week_num)])

    logger.info("{current_datetime}, build {database_name}.{ticket_features_table_name} end".format(current_datetime=datetime.now(),
                                                                                                    database_name=database_name,
                                                                                                    ticket_features_table_name=ticket_features_table_name))

    logger.info("{current_datetime}, end".format(current_datetime=datetime.now()))


# def calc_clusters():
#     from airflow.hooks.clickhouse_hook import ClickHouseHook
#
#     from models.item_clustering.clustering import ModelKMeans
#     from models.item_clustering.clustering_config import ModelKMeansConfig
#
#     client_clickhouse = ClickHouseHook(clickhouse_conn_id='clickhouse_salesfs')
#     model_params= ModelKMeansConfig().model_hp
#     model = ModelKMeans(params=model_params)
#
#     sql_query = "select * from salesfs.SOURCE_TABLENAME"
#     data = client_clickhouse.get_pandas_df(sql_query)
#     data_clustered = model.cluster_items(data, "item_qty", "week_num")
#     data_clustered['source'] = 'calculation'
#
#     table_name = "TABLENAME_raw"
#     client_clickhouse.run(f"TRUNCATE TABLE {table_name}")
#     client_clickhouse.run(f"INSERT INTO {table_name} VALUES", data_clustered.to_dict('records'))


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

load_ticket_features = PythonOperator(
    task_id="load_ticket_features",
    python_callable=load_ticket_features,
    provide_context=True,
    dag=load_tickets_features_dag
)
build_item_qty_to_drop_outlier >> load_tickets >> load_ticket_features
