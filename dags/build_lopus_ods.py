import os
import logging
import pendulum
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
    'start_date': datetime(2020, 7, 14, tzinfo=LOCAL_TZ),
    'queue': MODULE_NAME,
    'concurrency': 10
}

build_lopus_ods_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="00 7 * * *",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def build_lopus_products(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_lopus_products'
    ods_table_name = 'ods_lopus_products'
    table_columns_comma_sep = 'product_id, attributes, stamp, load_dttm'
    business_key = 'product_id'
    business_dttm = 'stamp'

    # ===================================================LOAD TABLE====================================================

    load_increment_fact_table(logger=logger,
                              database_name=database_name,
                              source_table_name=raw_table_name,
                              target_table_name=ods_table_name,
                              target_table_columns_comma_sep=table_columns_comma_sep,
                              business_key=business_key,
                              business_dttm=business_dttm)

    # =====================================================END OPERATOR================================================


def build_lopus_families(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_lopus_families'
    ods_table_name = 'ods_lopus_families'
    table_columns_comma_sep = 'product_id, families, stamp, load_dttm'
    business_key = 'product_id'
    business_dttm = 'stamp'

    # ===================================================LOAD TABLE====================================================

    load_increment_fact_table(logger=logger,
                              database_name=database_name,
                              source_table_name=raw_table_name,
                              target_table_name=ods_table_name,
                              target_table_columns_comma_sep=table_columns_comma_sep,
                              business_key=business_key,
                              business_dttm=business_dttm)

    # =====================================================END OPERATOR================================================


def build_products_info(ds, **kwargs):

    from airflow_clickhouse_plugin import ClickHouseHook

    client = ClickHouseHook(clickhouse_conn_id='clickhouse_salesfs')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_products_info'
    target_table_name = 'ods_products_info'
    target_alias = 'trg'
    increment_alias = 'inc'
    business_key = 'item'
    business_dttm = 'created_dttm'
    valid_from_tech_field = 'valid_from_dttm'
    valid_to_tech_field = 'valid_to_dttm'
    updated_tech_field = 'updated_dttm'
    is_actual_tech_filed = 'is_actual'
    load_tech_field = 'load_dttm'
    business_columns_comma_sep = 'item, name, model, families, type, gamma, dep, subdep, class, subclass, created_dttm'
    increment_days = '7'

    # =================================================================================================================

    table_name_hist_temp = target_table_name + '_hist_temp'
    tech_columns_comma_sep = '''{valid_from_tech_field}, {valid_to_tech_field}, {updated_tech_filed}, {is_actual_tech_filed}, {load_tech_field}'''.format(
        valid_from_tech_field=valid_from_tech_field,
        valid_to_tech_field=valid_to_tech_field,
        updated_tech_filed=updated_tech_field,
        is_actual_tech_filed=is_actual_tech_filed,
        load_tech_field=load_tech_field)

    # Таблица, в которую записываются четыре типа версий по ключу:
    #   1. Новые версии по ключу, пришедшие в raw слой;
    #   2. Старые актуальные версии по ключу, которые надо "закрыть". В них обновляются поля valid_from_dttm, valid_to_dttm, updated_dttm и is_actual;
    #   3. Старые, уже закрытые версии по ключу версии с is_actual = 0;
    #   4. Открытые версии, для которых по ключу не пришло новых версий.
    # Эта таблица полностью перезаписывает target-таблицу

    CREATE_HIST_TEMP_SQL = """ 
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
        ENGINE = MergeTree()
        ORDER BY ({business_key}, {valid_from_tech_field})
        SETTINGS index_granularity = 8192
        AS SELECT {business_columns_comma_sep}, {tech_columns_comma_sep} 
             FROM {database_name}.{target_table_name} 
             WHERE 1=0            
        """
    # Создание raw-слоя для products_info. В параметризации не нуждается из-за особенности бизнес-логики составления raw-таблицы

    BUILD_RAW_SQL = """ 
    	insert into salesfs.raw_products_info (item, name, model, families, type, gamma, dep, subdep, class, subclass, valid_from_dttm, valid_to_dttm, created_dttm, updated_dttm, is_actual)
    		select 
    			toUInt32OrNull(product_id) as item,
    			JSONExtract(attributes, '12963', 1, 'Nullable(String)') as name,
    			toUInt32OrNull(JSONExtract(attributes, 'model_modelID', 1, 'Nullable(String)')) as model,
    			fam.families as families,
    			JSONExtract(attributes, '01181', 1, 'Nullable(String)') as type,
    			JSONExtract(attributes, 'letterRange', 1, 'Nullable(String)') as gamma,
    			toUInt8OrNull(JSONExtract(attributes, 'mgtNomencProduct_mgtNomenc_departmentID', 1, 'Nullable(String)')) as dep,
    			toUInt16OrNull(JSONExtract(attributes, 'mgtNomencProduct_mgtNomenc_subDepartmentID', 1, 'Nullable(String)')) as subdep,
    			toUInt16OrNull(JSONExtract(attributes, 'mgtNomencProduct_mgtNomenc_categoryID', 1, 'Nullable(String)')) as class,
    			toUInt16OrNull(JSONExtract(attributes, 'mgtNomencProduct_mgtNomenc_subCategoryID', 1, 'Nullable(String)')) as subclass,
    			case when attr.valid_from_dttm <= fam.valid_from_dttm and fam.valid_from_dttm is not null then fam.valid_from_dttm else attr.valid_from_dttm end as valid_from_dttm,
    			case when attr.valid_to_dttm <= fam.valid_to_dttm and fam.valid_to_dttm is not null then fam.valid_to_dttm else attr.valid_to_dttm end as valid_to_dttm,
    			case when attr.stamp <= fam.stamp and fam.stamp is not null then fam.stamp else attr.stamp end as created_dttm,
    			case when attr.updated_dttm <= fam.updated_dttm and fam.updated_dttm is not null then fam.updated_dttm else attr.updated_dttm end as updated_dttm,
    			'1' as is_actual
    		from salesfs.ods_lopus_products attr
    		left join (
    			select 
    				product_id,
    				arrayFilter((fam, ftype) -> ftype = 'STANDARD', 
    					arrayMap(i -> JSONExtract(families, i + 1, 'id', 'String'), range(JSONLength(families))),
    					arrayMap(i -> JSONExtract(families, i + 1, 'type', 'String'), range(JSONLength(families)))) as families,
    				valid_from_dttm,
    				valid_to_dttm,
    				stamp,
    				updated_dttm
    			from (
    				select product_id, families, valid_from_dttm, valid_to_dttm, stamp, updated_dttm
    				from salesfs.ods_lopus_families 
    				where JSONLength(families) > 0
    				)
    			) fam
    		on attr.product_id = fam.product_id
    		where item is not null
    			and created_dttm >= now() - interval '{increment_days}' day;
    	""".format(increment_days=increment_days)

    GET_COUNT_SQL = ''' SELECT COUNT(*) FROM {database_name}.{table_name_hist_temp} '''

    build_tech_fields = '''
                        CASE
                            WHEN {target_alias}.{valid_from_tech_field} >= {increment_alias}.{valid_from_tech_field}
                            THEN {increment_alias}.{valid_to_tech_field} + interval '1' second
                            ELSE {target_alias}.{valid_from_tech_field}
                        END AS {valid_from_tech_field},
                        CASE
                            WHEN {target_alias}.{valid_to_tech_field} <= {increment_alias}.{valid_to_tech_field}
                            THEN {increment_alias}.{valid_from_tech_field} - interval '1' second
                            ELSE {target_alias}.{valid_to_tech_field}
                        END AS {valid_to_tech_field},
                        now() as {updated_tech_filed},
                        CASE 
                            WHEN {target_alias}.{is_actual_tech_filed} = '1'
                            THEN '0'
                            ELSE {target_alias}.{is_actual_tech_filed}
                        END AS {is_actual_tech_filed},
                        {target_alias}.{load_tech_field}'''.format(target_alias=target_alias,
                                                                   increment_alias=increment_alias,
                                                                   valid_from_tech_field=valid_from_tech_field,
                                                                   valid_to_tech_field=valid_to_tech_field,
                                                                   updated_tech_filed=updated_tech_field,
                                                                   is_actual_tech_filed=is_actual_tech_filed,
                                                                   load_tech_field=load_tech_field)

    build_target_hist = '''
    		SELECT DISTINCT * FROM (
    			SELECT 
    			{business_key},
    			CASE 
    				WHEN first_flg = '0'
    				THEN if(neighbor({business_key}, 1, NULL) != {business_key},  neighbor({valid_from_tech_field}, -1, NULL), NULL)
    				ELSE {valid_from_tech_field}
    			END AS {valid_from_tech_field},
    			CASE 
    				WHEN last_flg = '0'
    				THEN if(neighbor({business_key}, -1, NULL) != {business_key},  neighbor({valid_to_tech_field}, 1, NULL), NULL)
    				ELSE {valid_to_tech_field}
    			END AS {valid_to_tech_field}
    			FROM (
    				SELECT 
    					{business_key},
    					{valid_from_tech_field},
    					{valid_to_tech_field},
    					if(neighbor({business_key}, 1, NULL) != {business_key}, '1', '0') AS last_flg,
    					if(neighbor({business_key}, -1, NULL) != {business_key}, '1', '0') AS first_flg
    				FROM (
    					SELECT 
    						{business_key},
    						{valid_from_tech_field},
    						{valid_to_tech_field}
    					FROM {database_name}.{raw_table_name}
    					ORDER BY {business_key}, {valid_from_tech_field}
    					)
    				)
    			WHERE last_flg = '1' OR first_flg = '1'
    		)'''.format(database_name=database_name,
                        raw_table_name=raw_table_name,
                        business_key=business_key,
                        valid_from_tech_field=valid_from_tech_field,
                        valid_to_tech_field=valid_to_tech_field)

    BUILD_ODS_HIST_TEMP_SQL = """ 
        INSERT INTO {database_name}.{table_name_hist_temp} ({business_columns_comma_sep}, {tech_columns_comma_sep})
            SELECT 
                {business_columns_comma_sep}, 
                {tech_columns_comma_sep} 
            FROM (
                -- Новые версии по ключу
                SELECT 
                    {business_columns_comma_sep}, 
                    {tech_columns_comma_sep} 
                FROM {database_name}.{raw_table_name}
                UNION ALL
                -- Старые актуальные версии по ключу, которые надо "закрыть"
                SELECT * FROM (
                    SELECT 
                        {build_business_fields_with_alias},
                        {build_tech_fields}
                    FROM {database_name}.{target_table_name} {target_alias}
                    INNER JOIN (
                        {build_target_hist}
                        ) {increment_alias}
                        ON {target_alias}.{business_key} = {increment_alias}.{business_key} 
                    ) 
                WHERE {valid_from_tech_field} < {valid_to_tech_field}
                ORDER BY {business_key}, {valid_from_tech_field} DESC
                LIMIT 1 BY {business_key}
                UNION ALL	
                -- Старые, уже закрытые версии по ключу версии с is_actual = 0
                SELECT 
                    {build_business_fields_with_alias},
                    {build_tech_fields_with_alias}
                FROM {database_name}.{target_table_name} {target_alias}
                INNER JOIN (
                    {build_target_hist}
                    ) {increment_alias}
                    ON {target_alias}.{business_key} = {increment_alias}.{business_key} 
                WHERE {target_alias}.{is_actual_tech_filed} = '0'
                UNION ALL
                -- Открытые версии, для которых по ключу не пришло новых версий
                SELECT 
                    {build_business_fields_with_alias},
                    {build_tech_fields_with_alias}
                FROM {database_name}.{target_table_name} {target_alias}
                LEFT JOIN {database_name}.{raw_table_name} {increment_alias}
                ON  {target_alias}.{business_key} = {increment_alias}.{business_key}
                WHERE {increment_alias}.{is_actual_tech_filed} = ''
                )
        """.format(database_name=database_name,
                   target_table_name=target_table_name,
                   raw_table_name=raw_table_name,
                   table_name_hist_temp=table_name_hist_temp,
                   target_alias=target_alias,
                   increment_alias=increment_alias,
                   business_key=business_key,
                   business_columns_comma_sep=business_columns_comma_sep,
                   build_business_fields_with_alias=', '.join(
                       [target_alias + '.' + column for column in business_columns_comma_sep.split(', ')]),
                   build_tech_fields_with_alias=', '.join(
                       [target_alias + '.' + column for column in tech_columns_comma_sep.split(', ')]),
                   valid_from_tech_field=valid_from_tech_field,
                   valid_to_tech_field=valid_to_tech_field,
                   is_actual_tech_filed=is_actual_tech_filed,
                   tech_columns_comma_sep=tech_columns_comma_sep,
                   build_tech_fields=build_tech_fields,
                   build_target_hist=build_target_hist
                   )

    # Отсеивание дублей

    table_name_hist_dedup = table_name_hist_temp + '_dedup'

    CREATE_DEDUPLICATION_TABLE_SQL = """ 
                               CREATE TABLE IF NOT EXISTS {database_name}.{table_name_hist_dedup}
                               ENGINE = MergeTree()
                               ORDER BY ({business_key}, {valid_from_tech_field})
                               SETTINGS index_granularity = 8192
                               AS SELECT {business_columns_comma_sep}, {tech_columns_comma_sep} FROM {database_name}.{table_name_hist_temp} WHERE 1=0            
                               """.format(database_name=database_name,
                                          table_name_hist_temp=table_name_hist_temp,
                                          table_name_hist_dedup=table_name_hist_dedup,
                                          business_key=business_key,
                                          valid_from_tech_field=valid_from_tech_field,
                                          business_columns_comma_sep=business_columns_comma_sep,
                                          tech_columns_comma_sep=tech_columns_comma_sep)

    BUILD_DEDUPLICATION_TABLE_SQL = """ INSERT INTO {database_name}.{table_name_hist_dedup} ({business_columns_comma_sep}, {tech_columns_comma_sep})
                                SELECT {business_columns_comma_sep}, {tech_columns_comma_sep} 
                                FROM {database_name}.{table_name_hist_temp}
                                ORDER BY {business_key}, {valid_from_tech_field}, {business_dttm} DESC, {updated_tech_field}
                                LIMIT 1 BY ({business_key}, {valid_from_tech_field}) 
                                """.format(database_name=database_name,
                                           table_name_hist_dedup=table_name_hist_dedup,
                                           table_name_hist_temp=table_name_hist_temp,
                                           business_columns_comma_sep=business_columns_comma_sep,
                                           tech_columns_comma_sep=tech_columns_comma_sep,
                                           business_key=business_key,
                                           business_dttm=business_dttm,
                                           valid_from_tech_field=valid_from_tech_field,
                                           updated_tech_field=updated_tech_field)

    TRUNCATE_SQL = """ TRUNCATE TABLE IF EXISTS {database_name}.{table_name} """

    DROP_SQL = """ DROP TABLE IF EXISTS {database_name}.{table_name} """

    BUILD_ODS_SQL = '''
        INSERT INTO {database_name}.{target_table_name} ({business_columns_comma_sep}, {tech_columns_comma_sep}) 
            SELECT 
                {business_columns_comma_sep}, 
                {tech_columns_comma_sep} 
            FROM 
                {database_name}.{table_name_hist_temp}
        '''.format(database_name=database_name,
                   target_table_name=target_table_name,
                   table_name_hist_temp=table_name_hist_dedup,
                   business_columns_comma_sep=business_columns_comma_sep,
                   tech_columns_comma_sep=tech_columns_comma_sep)

    # =================================================QUERY EXECUTION=================================================

    logger.info("{0}, insert into {1}.{2}".format(datetime.now(),
                                                  database_name,
                                                  raw_table_name))
    client.run(BUILD_RAW_SQL)

    logger.info("{0}, create {1}.{2}".format(datetime.now(),
                                             database_name,
                                             table_name_hist_temp))

    client.run(CREATE_HIST_TEMP_SQL.format(database_name=database_name,
                                           table_name=table_name_hist_temp,
                                           target_table_name=target_table_name,
                                           business_key=business_key,
                                           valid_from_tech_field=valid_from_tech_field,
                                           business_columns_comma_sep=business_columns_comma_sep,
                                           tech_columns_comma_sep=tech_columns_comma_sep))

    logger.info("{0}, check count for {1}.{2}".format(datetime.now(),
                                                      database_name,
                                                      table_name_hist_temp))

    count_hist_temp = client.run(GET_COUNT_SQL.format(database_name=database_name,
                                                      table_name_hist_temp=table_name_hist_temp))
    if count_hist_temp != 0:
        logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                                   database_name,
                                                   table_name_hist_temp))
        client.run(TRUNCATE_SQL.format(database_name=database_name,
                                       table_name=table_name_hist_temp))

    # Объединение истории
    logger.info("{0}, build {1}.{2}".format(datetime.now(),
                                            database_name,
                                            table_name_hist_temp))
    logger.info("{0}, QUERY: {BUILD_ODS_HIST_TEMP_SQL}".format(datetime.now(),
                                                               BUILD_ODS_HIST_TEMP_SQL=BUILD_ODS_HIST_TEMP_SQL))
    client.run(BUILD_ODS_HIST_TEMP_SQL)

    # Дедупликация объединенной истории
    client.run(CREATE_DEDUPLICATION_TABLE_SQL)

    count_hist_temp_dedup = client.run(GET_COUNT_SQL.format(database_name=database_name,
                                                            table_name_hist_temp=table_name_hist_dedup))
    if count_hist_temp_dedup != 0:
        logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                                   database_name,
                                                   table_name_hist_temp))
        client.run(TRUNCATE_SQL.format(database_name=database_name,
                                       table_name=table_name_hist_dedup))

    client.run(BUILD_DEDUPLICATION_TABLE_SQL)

    # Загрузка в ODS
    logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                               database_name,
                                               target_table_name))
    client.run(TRUNCATE_SQL.format(database_name=database_name,
                                   table_name=target_table_name))

    logger.info("{0}, insert into {1}.{2}".format(datetime.now(),
                                                  database_name,
                                                  target_table_name))
    logger.info("{0}, QUERY: {BUILD_ODS_SQL}".format(datetime.now(),
                                                     BUILD_ODS_SQL=BUILD_ODS_SQL))
    client.run(BUILD_ODS_SQL)

    logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                               database_name,
                                               raw_table_name))
    client.run(TRUNCATE_SQL.format(database_name=database_name,
                                   table_name=raw_table_name))

    logger.info("{0}, drop {1}.{2}".format(datetime.now(),
                                           database_name,
                                           table_name_hist_temp))
    client.run(DROP_SQL.format(database_name=database_name,
                               table_name=table_name_hist_temp))

    logger.info("{0}, drop {1}.{2}".format(datetime.now(),
                                           database_name,
                                           table_name_hist_temp))
    client.run(DROP_SQL.format(database_name=database_name,
                               table_name=table_name_hist_dedup))

    logger.info("{}, end".format(datetime.now()))

    # ==================================================END OPERATOR===================================================

build_lopus_products = PythonOperator(
    task_id="build_lopus_products", python_callable=build_lopus_products, provide_context=True, dag=build_lopus_ods_dag
)

build_lopus_families = PythonOperator(
    task_id="build_lopus_families", python_callable=build_lopus_families, provide_context=True, dag=build_lopus_ods_dag
)

build_products_info = PythonOperator(
    task_id="build_products_info", python_callable=build_products_info, provide_context=True, dag=build_lopus_ods_dag
)

[build_lopus_products, build_lopus_families] >> build_products_info
