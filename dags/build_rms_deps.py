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

build_rms_deps_dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="25 9 * * *",
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def gp_to_ch_rms_deps(ds, **kwargs):

    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.hooks.S3_hook import S3Hook
    from airflow_clickhouse_plugin import ClickHouseHook

    import psycopg2

    gp_client = PostgresHook(postgres_conn_id='gp_salesfs')
    s3_client = S3Hook(aws_conn_id='s3_salesfs')
    ch_client = ClickHouseHook(clickhouse_conn_id='clickhouse_salesfs')


    gp_database_name = 'rms_p009qtzb_rms_ods'
    gp_view_name = 'v_deps'
    ch_database_name = 'salesfs'
    ch_raw_table_name = 'raw_rms_deps'
    table_columns_comma_sep = "nk, dept, dept_name, buyer, merch, profit_calc_type, purchase_type, group_no, bud_int, bud_mkup, total_market_amt, markup_calc_type, otb_calc_type, max_avg_counter, avg_tolerance_pct, dept_vat_incl_ind, to_char(valid_from_dttm, 'YYYY-MM-DD HH24:MI:SS') as valid_from_dttm, to_char(created_dttm, 'YYYY-MM-DD HH24:MI:SS') as created_dttm, to_char(updated_dttm, 'YYYY-MM-DD HH24:MI:SS') as updated_dttm, is_actual"
    is_act_field = 'is_actual'
    business_dttm_field = 'created_dttm'
    increment_by_date = '7'
    s3_path = 's3://t-dacc-sfsm/etl/ods_rms_deps'
    s3_profile = ''
    s3_pxf_server = ''
    

    gp_ext_table_name = gp_view_name + '_dump'

    

    GET_COLUMNS_DDL_GP = """
                        select string_agg(column_spec, ', ' || chr(10)) from
                            (select quote_ident(translate(c.column_name, '$', '_')) || ' ' ||
                                    case
                                        when c.data_type = 'smallint' then 'integer'
                                        when c.udt_name = 'time' then 'text'
                                        else c.udt_name
                                        end ||
                                    case
                                        when c.data_type in ('character', 'character varying') and c.character_maximum_length is not null then '(' || c.character_maximum_length || ')'
                                        when c.data_type = 'numeric' and c.numeric_precision is not null then '(' || c.numeric_precision || ',' || c.numeric_scale || ')'
                                        else ''
                                        end as column_spec
                             from information_schema.columns c where c.table_schema = '{gp_database_name}' and c.table_name = '{gp_view_name}'
                             order by c.ordinal_position) q
                         """.format(gp_database_name=gp_database_name,
                                    gp_view_name=gp_view_name)

    # cursor = gp_client.cursor()
    # cursor.execute(GET_COLUMNS_DDL_GP)
    # ddl_columns_gp = cursor.fetchall()[0][0]

    CREATE_EXT_TABLE = """
                        DROP EXTERNAL TABLE IF EXISTS {gp_ext_database}.{gp_view_name};
                        CREATE WRITABLE EXTERNAL TEMPORARY TABLE {gp_ext_database}.{gp_view_name} 
                          ({ddl_columns_gp})
                        LOCATION(''pxf://' || regexp_replace({s3_path}, '\w+://', '') || '?PROFILE=' || {s3_profile} || '&SERVER=' || {s3_pxf_server} || ''
                        '&COMPRESSION_CODEC=' || p_compression_codec || '&CODEC_LEVEL=' || p_codec_level;
                        v_sql := v_sql || ''')' || chr(10) || 'FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_export'')' || chr(10)
                || lm_toolkit.fn_get_distribution_clause(p_schema_name, p_table_name) || ';';
                        """.format(gp_ext_database=gp_database_name, gp_view_name=gp_ext_table_name, ddl_columns_gp=ddl_columns_gp, s3_path=s3_path,
                                   s3_profile=s3_profile, s3_pxf_server=s3_pxf_server)
    print(CREATE_EXT_TABLE)

    """
    create or replace function fn_dump(p_schema_name text,
                                   p_table_name text,
                                   p_path text,
                                   p_incremental boolean default false,
                                   p_bound_attr text default null,
                                   p_lower_bound text default null,
                                   p_upper_bound text default null,
                                   p_profile text default 's3:avro',
                                   p_compression_codec text default 'uncompressed',
                                   p_codec_level text default '6',
                                   p_pxf_server text default 's3srv')
    returns bigint as
$fn_dump$
declare
    v_sql text;
    v_cnt bigint;
begin
    v_sql := 'DROP EXTERNAL TABLE IF EXISTS tmp_dump;
    CREATE WRITABLE EXTERNAL TEMPORARY TABLE tmp_dump (
    ' || lm_toolkit.fn_get_casted_ddl_as_text(p_schema_name, p_table_name) || '
    )
    LOCATION(''pxf://' || regexp_replace(s3://dummy, '\w+://', '') || '?PROFILE=' || p_profile || '&SERVER=' || p_pxf_server || '';
    
    v_sql := v_sql || '&COMPRESSION_CODEC=' || p_compression_codec || '&CODEC_LEVEL=' || p_codec_level;
    v_sql := v_sql || ''')' || chr(10) || 'FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_export'')' || chr(10)
                || lm_toolkit.fn_get_distribution_clause(p_schema_name, p_table_name) || ';';
    raise notice '%', v_sql;
    execute v_sql;
    v_sql := 'INSERT INTO tmp_dump SELECT * FROM ' || p_schema_name || '.' || p_table_name;
    if p_incremental and p_bound_attr != '' then
        v_sql := v_sql || ' where 1=1';
        if p_lower_bound is not null and p_lower_bound != '' then
            v_sql := v_sql || ' and ' || quote_ident(p_bound_attr) || ' > ' || quote_literal(p_lower_bound) || '::timestamp without time zone';
        end if;
        if p_upper_bound is not null and p_upper_bound != '' then
            v_sql := v_sql || ' and ' || quote_ident(p_bound_attr) || ' <= ' || quote_literal(p_upper_bound) || '::timestamp without time zone';
        end if;
    end if;
    v_sql = v_sql || ';';
    raise notice '%', v_sql;
    execute v_sql;
    get diagnostics v_cnt=row_count;
    raise notice 'Execute: %', v_cnt;
    return v_cnt;
end
$fn_dump$
    language plpgsql;
-- security definer;

    """

    #cursor.execute(CREATE_EXT_TABLE)
    #cursor.commit()


    DUMP_QUERY = """
                    select lm_toolkit.fn_dump(p_schema_name := 'scheme', 
                                              p_table_name := 'table', 
                                              p_path := 's3://dummy', 
                                              p_incremental := false, 
                                              p_profile := 's3:dummy', 
                                              p_compression_codec := 'comressed', 
                                              p_codec_level := '7', 
                                              p_pxf_server := 's3srvFake');
                 """




def build_rms_deps(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_rms_deps'
    ods_table_name = 'ods_rms_deps'
    table_columns_comma_sep = 'nk, dept, dept_name, buyer, merch, profit_calc_type, purchase_type, group_no, bud_int, bud_mkup, total_market_amt, markup_calc_type, otb_calc_type, max_avg_counter, avg_tolerance_pct, dept_vat_incl_ind, valid_from_dttm, created_dttm, updated_dttm, is_actual, load_dttm'
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

# gp_to_ch_rms_deps = PythonOperator(
#     task_id="gp_to_ch_rms_deps", python_callable=gp_to_ch_rms_deps, provide_context=True, dag=build_rms_deps_dag
# )

build_rms_deps = PythonOperator(
    task_id="build_rms_deps", python_callable=build_rms_deps, provide_context=True, dag=build_rms_deps_dag
)

build_rms_deps