from datetime import datetime


def load_increment_fact_table(logger,
                              database_name, 
                              source_table_name, 
                              target_table_name, 
                              target_table_columns_comma_sep,
                              business_key,
                              business_dttm=None,
                              is_act_field=None,
                              create_nk_field=None,
                              mapping_select_to_target=None):
    
    from sf_api.run_clickhouse import run_clickhouse_query

    is_act_where_clause = ''
    business_dttm_order_clause = ''
    if is_act_field is not None:
        is_act_where_clause = "and {is_act_field} = '1'".format(is_act_field=is_act_field)
    if business_dttm is not None:
        business_dttm_order_clause = ", {business_dttm} DESC".format(business_dttm=business_dttm)
    if create_nk_field is not None:
        source_table_columns_comma_sep = target_table_columns_comma_sep.replace('nk', create_nk_field)
    else:
        source_table_columns_comma_sep = target_table_columns_comma_sep
    if mapping_select_to_target is None:
        mapping_select_to_target = target_table_columns_comma_sep

    # ===================================================QUERY INIT====================================================

    source_table_name_temp = source_table_name + '_temp'

    CREATE_DEDUPLICATION_TEMP_SQL = """ 
                    CREATE TABLE IF NOT EXISTS {database_name}.{source_table_name_temp}
                    ENGINE = MergeTree()
                    ORDER BY ({business_key})
                    SETTINGS index_granularity = 8192
                    AS SELECT {source_table_columns_comma_sep} FROM {database_name}.{source_table_name} WHERE 1=0            
                    """

    BUILD_DEDUPLICATION_TEMP_SQL = """ 
                        INSERT INTO {database_name}.{source_table_name_temp} ({target_table_columns_comma_sep})
                             SELECT {target_table_columns_comma_sep} FROM (
                                 SELECT {source_table_columns_comma_sep} FROM {database_name}.{source_table_name}
                                 WHERE 1=1
                                 UNION ALL
                                 SELECT {target_table_columns_comma_sep} FROM {database_name}.{target_table_name}
                                 WHERE 1=1
                             )
                             ORDER BY {business_key} {business_dttm_order_clause}
                             LIMIT 1 BY ({business_key})
                        """

    BUILD_ODS_SQL = """ INSERT INTO {database_name}.{target_table_name} ({target_table_columns_comma_sep})
                            SELECT {mapping_select_to_target} FROM {database_name}.{source_table_name_temp} """

    GET_COUNT_SQL = ''' SELECT COUNT(*) FROM {database_name}.{source_table_name_temp} '''

    TRUNCATE_SQL = """ TRUNCATE TABLE IF EXISTS {database_name}.{table_name} """

    DROP_SQL = """ DROP TABLE IF EXISTS {database_name}.{table_name} """

    CREATE_VIEW_SQL = """CREATE VIEW IF NOT EXISTS {database_name}.view_{target_table_name} AS SELECT {target_table_columns_comma_sep} FROM {database_name}.{target_table_name} where 1=1 {is_act_where_clause}"""

    # =================================================QUERY EXECUTION=================================================
    
    logger.info("{0}, create {1}.{2}".format(datetime.now(),
                                             database_name,
                                             source_table_name_temp))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[CREATE_DEDUPLICATION_TEMP_SQL.format(database_name=database_name,
                                                                                     source_table_name_temp=source_table_name_temp,
                                                                                     business_key=business_key,
                                                                                     source_table_columns_comma_sep=source_table_columns_comma_sep,
                                                                                     source_table_name=source_table_name)])

    logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                               database_name,
                                               source_table_name_temp))

    logger.info("{0}, insert deduplicated data into {1}.{2}".format(datetime.now(),
                                                                    database_name,
                                                                    source_table_name_temp))

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=source_table_name_temp),
                                                BUILD_DEDUPLICATION_TEMP_SQL.format(database_name=database_name,
                                                                                    source_table_name_temp=source_table_name_temp,
                                                                                    source_table_name=source_table_name,
                                                                                    source_table_columns_comma_sep=source_table_columns_comma_sep,
                                                                                    target_table_name=target_table_name,
                                                                                    target_table_columns_comma_sep=target_table_columns_comma_sep,
                                                                                    business_key=business_key,
                                                                                    business_dttm_order_clause=business_dttm_order_clause)])

    logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                               database_name,
                                               target_table_name))

    logger.info("{0}, insert into {1}.{2}".format(datetime.now(),
                                                  database_name,
                                                  target_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=target_table_name),
                                                BUILD_ODS_SQL.format(database_name=database_name,
                                                                     target_table_name=target_table_name,
                                                                     source_table_name_temp=source_table_name_temp,
                                                                     target_table_columns_comma_sep=target_table_columns_comma_sep,
                                                                     mapping_select_to_target=mapping_select_to_target)])

    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[CREATE_VIEW_SQL.format(database_name=database_name,
                                                                       target_table_name=target_table_name,
                                                                       target_table_columns_comma_sep=target_table_columns_comma_sep,
                                                                       is_act_where_clause=is_act_where_clause)])

    logger.info("{0}, truncate {1}.{2}".format(datetime.now(),
                                               database_name,
                                               source_table_name))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[TRUNCATE_SQL.format(database_name=database_name,
                                                                    table_name=source_table_name)])

    logger.info("{0}, drop {1}.{2}".format(datetime.now(),
                                           database_name,
                                           source_table_name_temp))
    run_clickhouse_query(logger=logger,
                         clickhouse_query_list=[DROP_SQL.format(database_name=database_name,
                                                                table_name=source_table_name_temp)])

    logger.info("{0}, END".format(datetime.now()))

