import traceback
import time
import sys


def run_clickhouse_query(clickhouse_query_list,
                         logger,
                         max_memory_usage_for_user=None,
                         max_memory_usage=None,
                         max_query_size=None,
                         error_cnt=1,
                         return_result=False):

    from airflow_clickhouse_plugin import ClickHouseHook
    
    client = ClickHouseHook(clickhouse_conn_id='clickhouse_salesfs')

    max_error_cnt = 3
    error_flg = False
    default_max_memory_usage_for_user = 0
    default_max_memory_usage = 0
    default_max_query_size = 0

    try:

        default_max_memory_usage_for_user = int(
            client.run("SELECT value FROM system.settings WHERE name = 'max_memory_usage_for_user'")[0][0])
        default_max_memory_usage = int(
            client.run("SELECT value FROM system.settings WHERE name = 'max_memory_usage'")[0][0])
        default_max_query_size = int(
            client.run("SELECT value FROM system.settings WHERE name = 'max_query_size'")[0][0])

        if max_memory_usage_for_user is None:
            max_memory_usage_for_user = default_max_memory_usage_for_user
        if max_memory_usage is None:
            max_memory_usage = default_max_memory_usage
        if max_query_size is None:
            max_query_size = default_max_query_size

        if return_result:
            return [client.run(clickhouse_query) for clickhouse_query in clickhouse_query_list]
        else:
            for clickhouse_query in clickhouse_query_list:
                client.run("set max_memory_usage_for_user = {0}".format(max_memory_usage_for_user))
                client.run("set max_memory_usage = {0}".format(max_memory_usage))
                client.run("set max_query_size = {0}".format(max_query_size))
                client.run(clickhouse_query)

    except Exception:
        error_text = traceback.format_exc()
        error_flg = True
        logger.info(error_text)
        logger.info('Error count: {0}'.format(error_cnt))

        if error_text.find('Memory limit (for user) exceeded') != -1:
            if max_memory_usage_for_user == default_max_memory_usage_for_user:
                max_memory_usage_for_user = default_max_memory_usage_for_user * 1.5
            if max_memory_usage == default_max_memory_usage:
                max_memory_usage = default_max_memory_usage * 1.5
            if max_query_size == default_max_query_size:
                max_query_size = default_max_query_size * 1.5

    if error_flg:
        if error_cnt < max_error_cnt:

            # Задержка на 10 секунд, чтобы у других запросов был шанс отработать
            time.sleep(10)

            error_cnt += 1
            run_clickhouse_query(clickhouse_query_list=clickhouse_query_list,
                                 logger=logger,
                                 max_memory_usage_for_user=max_memory_usage_for_user,
                                 max_memory_usage=max_memory_usage,
                                 max_query_size=max_query_size,
                                 error_cnt=error_cnt,
                                 return_result=return_result)
        else:
            sys.exit(error_text)

