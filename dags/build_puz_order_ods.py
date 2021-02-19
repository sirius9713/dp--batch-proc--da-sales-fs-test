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
    'retries': 10,
    'retry_delay': timedelta(minutes=15),
    'start_date': airflow.utils.dates.days_ago(1),
    'queue': MODULE_NAME,
    'concurrency': 10
}


build_puz_order_ods_dag=DAG(
    dag_id=DAG_ID,
    default_args=args,
    max_active_runs=1,
    schedule_interval="00 4 * * *", 
    catchup=True,
    access_control={
        'sales-fs': {'can_dag_read', 'can_dag_edit'}
    }
)


def build_puz_order_ods(ds, **kwargs):

    from sf_api.table_loader import load_increment_fact_table

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # ===================================================PARAMETERS====================================================

    database_name = 'salesfs'
    raw_table_name = 'raw_puz_order'
    ods_table_name = 'ods_puz_order'
    table_columns_comma_sep = '''
                                   nk, id, ordernumber, pyxisid, bitrixid, bitrixaccountnumber, createdat, transfernumber,
                                   url, status, statusdate, region_id, region_name, region_code, region_deleted, store_id,
                                   store_name, store_additionalcode, store_deleted, store_regionlogicnumber,
                                   store_region_id, store_region_name, store_region_code, store_region_deleted,
                                   source_saleschannel, source_referral, source_devicetype_id, source_devicetype_name,
                                   source_operator_name, source_operator_surname, source_operator_fullname,
                                   source_coordinates, operatorcomment, assortmentcomment, logisticiancomment, customer_id,
                                   customer_customernumber, customer_bitrixid, customer_name, customer_surname,
                                   customer_fullname, customer_phone, customer_email, customer_region_id,
                                   customer_region_code, customer_region_deleted, customer_region_name, customercomment,
                                   payment_status_code, payment_status_name, payment_type_code, payment_type_name,
                                   payment_method_code, payment_method_name, payment_handler, payment_date,
                                   processing_status_code, processing_status_name, processing_startedat,
                                   processing_duration, processing_operator_name, processing_operator_surname,
                                   processing_operator_fullname, picking_status_code, picking_status_name,
                                   picking_startedat, picking_duration, shipment_status_code, shipment_status_name,
                                   shipment_startedat, shipment_duration, delivery_status_code, delivery_status_name,
                                   delivery_startedat, delivery_duration, delivery_endedat, delivery_type_code,
                                   delivery_type_name, delivery_date, delivery_time, delivery_serviceinfo_number,
                                   delivery_serviceinfo_paid, delivery_serviceinfo_dateplanned,
                                   deliveryserviceinfo_currentstatus_id, deliveryserviceinfo_currentstatus_date,
                                   deliveryserviceinfo_currentstatus_createdat, deliveryserviceinfo_currentstatusds_id,
                                   deliveryserviceinfo_currentstatusds_name, deliveryserviceinfo_currentstatusds_externalid,
                                   deliveryserviceinfo_currentstatusasd_id, deliveryserviceinfo_currentstatusasd_name,
                                   deliveryserviceinfo_currentstatusasd_code, deliveryserviceinfo_carrier_id,
                                   deliveryserviceinfo_carrier_code, deliveryserviceinfo_carrier_name,
                                   deliveryserviceinfo_carrier_service_id, deliveryserviceinfo_carrier_service_code,
                                   deliveryserviceinfo_carrier_service_name, deliveryserviceinfo_deliveryservice_id,
                                   deliveryserviceinfo_deliveryservice_code, deliveryserviceinfo_deliveryservice_name,
                                   deliveryserviceinfo_services_deliveredquantitytotal, delivery_address_province,
                                   delivery_address_district, delivery_address_city, delivery_address_street,
                                   delivery_address_house, delivery_address_porch, delivery_address_floor,
                                   delivery_address_apartment, delivery_address_intercom, delivery_address_full,
                                   delivery_address_coordinates, delivery_deliverypoint_code, delivery_deliverypoint_phone,
                                   deliverypoint_address_full, deliverypoint_address_city, deliverypoint_address_area,
                                   delivery_deliverypoint_businesshours, delivery_carrier_id, delivery_carrier_code,
                                   delivery_carrier_name, delivery_carryingdistance, delivery_liftupneeded,
                                   delivery_elevatoravailable, delivery_sameday, delivery_isextrabig, delivery_isextrabig2,
                                   delivery_volume, delivery_tax, delivery_deliverytype_code, delivery_deliverytype_name,
                                   delivery_destination, delivery_pickuptype, delivery_diff_deliveryamount,
                                   delivery_diff_deliveryamountreason, delivery_diff_liftamount,
                                   delivery_diff_liftamountreason, delivery_paymentregistry_date,
                                   delivery_paymentregistry_number, cancellation_status_code, cancellation_status_name,
                                   refund_status_code, refund_status_name, return_status_code, return_status_name,
                                   amounts_products, amounts_delivery, amounts_carrying, amounts_lift,
                                   amounts_deliverytotal, amounts_total, amounts_prepay, amounts_postpayproducts,
                                   amounts_postpaydeliverytotal, amounts_initial_total, amounts_initial_delivery,
                                   amounts_initial_lift, totals_weight, totals_volume, totals_productscount,
                                   totals_extrabigcount, totals_extralengthycount, totals_extravolumecount, totals_totext,
                                   totals_quantitydelivered, totals_longtail, tpnet_state, tpnet_message, tpnet_statedate,
                                   updatedat, versiondata, created_dttm, load_dttm'''
    business_key = 'id'
    create_nk_field = "concat(toString(id),'|',toString(parseDateTimeBestEffort(updatedat))) AS nk"
    business_dttm = 'created_dttm'

    # ===================================================LOAD TABLE====================================================

    load_increment_fact_table(logger=logger,
                              database_name=database_name,
                              source_table_name=raw_table_name,
                              target_table_name=ods_table_name,
                              target_table_columns_comma_sep=table_columns_comma_sep,
                              business_key=business_key,
                              business_dttm=business_dttm,
                              create_nk_field=create_nk_field)

    # ==================================================END OPERATOR===================================================

build_puz_order_ods = PythonOperator(
    task_id="build_puz_order_ods", python_callable=build_puz_order_ods, provide_context=True, dag=build_puz_order_ods_dag
)

build_puz_order_ods