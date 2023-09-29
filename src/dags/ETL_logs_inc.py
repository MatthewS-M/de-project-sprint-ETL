from datetime import datetime, timedelta
from re import T
import time
import psycopg2

import requests
import json
import pandas as pd
import numpy as np
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models.xcom import XCom
from airflow.models import Variable

# set user constants for api
NICKNAME = Variable.get('NICKNAME')
COHORT = Variable.get('COHORT')
http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

headers = {
    "X-API-KEY": api_key,
    "X-Nickname": NICKNAME,
    "X-Cohort": COHORT,
    'X-Project': 'True',
    'Content-Type': 'application/x-www-form-urlencoded'
}

# business_dt = '{{ ds }}'
date = datetime.today() 
one_day = timedelta(days=1)  
business_dt = date - one_day 
business_dt = business_dt.strftime("%Y-%m-%d")

task_logger = logging.getLogger("airflow.task")



### POSTGRESQL settings ###
# set postgresql connectionfrom basehook
postgres_conn = 'postgresql_de'
psql_conn = BaseHook.get_connection('postgresql_de')

# init test connection
conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()

# запрос выгрузки файлов
def _create_files_request(ti, base_url, headers):
    method_url = '/generate_report'
    response = requests.post(f'{base_url}{method_url}', headers=headers)
    response.raise_for_status()
    response_dict = json.loads(response.content)
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    task_logger.info(f'Response is {response.content}')


# проверка готовности файлов 
def check_report(ti, base_url, headers):
    task_ids = ti.xcom_pull(key='task_id')
    task_id = task_ids
    
    method_url = '/get_report'
    payload = {'task_id': task_id}

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        task_logger.info(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)


    if not report_id:
        raise TimeoutError('Some troubles with connection to API. Check once again your credentials and conn_link.')
    
    # если report_id не объявлен и success не было, то здесь возникнет ошибка
    ti.xcom_push(key='report_id', value=report_id)
    task_logger.info(f"Report_id is {report_id}")

def get_increment(date, ti, base_url, headers):
    task_logger.info('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    task_logger.info(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    task_logger.info(f'Increment_id is {increment_id}')

#загружаем файлы в таблицы stage   
def upload_from_s3_to_pg(ti,NICKNAME,COHORT,date):
    report_id = ti.xcom_pull(key='report_id')

    # insert to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # get order log
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{str(report_id)}/price_log.csv'
    local_filename = f"{date.replace('-', '')}_price_log.csv"
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    df_price_log = pd.read_csv(local_filename, names=['prod_name', 'price'])
    df_price_log.reset_index(drop = True, inplace = True)

    truncate_pl = "drop table if exists staging.price_log cascade;"
    cur.execute(truncate_pl)
    create_pl = "create table staging.price_log(id serial primary key, prod_name varchar(255), price integer);"
    cur.execute(create_pl)
    insert_pl = "insert into staging.price_log(prod_name, price) VALUES {pl_val};"
    i = 0
    step = int(df_price_log.shape[0] / 100)
    while i < df_price_log.shape[0]:
        print('df_price_log',i, end='\r')

        pl_val = str([tuple(x) for x in df_price_log.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_pl.replace('{pl_val}',pl_val))
        conn.commit()
        i += step+1

    ###
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{str(report_id)}/customer_research.csv'
    local_filename = f"{date.replace('-', '')}_customer_research.csv"
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    df_customer_research = pd.read_csv(local_filename)
    df_customer_research.reset_index(drop = True, inplace = True)
    insert_cr = "insert into staging.customer_research (date_id,category_id,geo_id,sales_qty,sales_amt) VALUES {cr_val};"
    i = 0
    step = int(df_customer_research.shape[0] / 100)
    while i <= df_customer_research.shape[0]:
        print('df_customer_research' , i, end='\r')
        
        cr_val =  str([tuple(x) for x in df_customer_research.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_cr.replace('{cr_val}',cr_val))
        conn.commit()
        i += step+1

    ###    
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{str(report_id)}/user_activity_log.csv'
    local_filename = f"{date.replace('-', '')}_user_activity_log.csv"
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    df_activity_log = pd.read_csv(local_filename)
    df_activity_log.reset_index(drop = True, inplace = True)
    insert_ual = "insert into staging.user_activity_log (uniq_id,date_time, action_id, customer_id, quantity) VALUES {ual_val};"
    i = 0
    step = int(df_activity_log.shape[0] / 100)
    while i < df_activity_log.shape[0]:
        print('df_activity_log',i, end='\r')
        
        ual_val =  str([tuple(x[1:]) for x in df_activity_log.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_ual.replace('{ual_val}',ual_val))
        conn.commit()
        i += step+1

    ###
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{str(report_id)}/user_order_log.csv'
    local_filename = f"{date.replace('-', '')}_user_order_log.csv"
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    df_order_log = pd.read_csv(local_filename)
    df_order_log.reset_index(drop = True, inplace = True)
    insert_uol = "insert into staging.user_order_log (id,uniq_id,date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount) VALUES {uol_val};"
    i = 0
    step = int(df_order_log.shape[0] / 100)
    while i <= df_order_log.shape[0]:
        print('df_order_log',i, end='\r')
        
        uol_val =  str([tuple(x) for x in df_order_log.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_uol.replace('{uol_val}',uol_val))
        conn.commit()        
        i += step+1

    cur.close()
    conn.close()
    return 200

#загружаем файлы в таблицы stage
def upload_inc_from_s3_to_pg(ti,NICKNAME,COHORT, date):
    increment_id = ti.xcom_pull(key='increment_id')

    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{str(increment_id)}/customer_research_inc.csv'
    local_filename = f"{date.replace('-', '')}_customer_research_inc.csv"
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    df_cr_temp = pd.read_csv(local_filename)

    sql = "truncate table staging.customer_research_temp;"
    cur.execute(sql)
    conn.commit()

    insert_cr = "insert into staging.customer_research_temp (date_id, category_id, geo_id, sales_qty, sales_amt) VALUES {cr_val};"
    step = int(df_cr_temp.shape[0] / 100)
    i=0
    while i < df_cr_temp.shape[0]:
        print(i, end='\r')
        s_val = str([tuple(x) for x in df_cr_temp.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_cr.replace('{cr_val}',s_val))
        conn.commit()
        i += step+1

    ###
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{str(increment_id)}/user_order_log_inc.csv'
    local_filename = f"{date.replace('-', '')}_user_order_log_inc.csv"
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    df_uol_temp = pd.read_csv(local_filename)
    df_uol_temp.reset_index(drop = True, inplace = True)
    sql = "truncate table staging.user_order_log_temp;"
    cur.execute(sql)
    conn.commit()

    insert_uol = "insert into staging.user_order_log_temp (id, uniq_id, date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount, status) VALUES {uol_val};"
    step = int(df_uol_temp.shape[0] / 100)
    i=0
    while i <= df_uol_temp.shape[0]:
        print(i, end='\r')
        s_val = str([tuple(x) for x in df_uol_temp.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_uol.replace('{uol_val}',s_val))
        conn.commit()
        i += step+1

    ###
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{str(increment_id)}/user_activity_log_inc.csv'
    local_filename = f"{date.replace('-', '')}_user_activity_log_inc.csv"
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    df_ual_temp = pd.read_csv(local_filename)
    df_ual_temp.reset_index(drop = True, inplace = True)
    sql = "truncate table staging.user_activity_log_temp;"
    cur.execute(sql)
    conn.commit()
    insert_ual = "insert into staging.user_activity_log_temp (id, uniq_id, date_time, action_id, customer_id, quantity) VALUES {ual_val};"
    step = int(df_ual_temp.shape[0] / 100)
    i=0
    while i < df_ual_temp.shape[0]:
        print(i, end='\r')
        s_val = str([tuple(x) for x in df_ual_temp.loc[i:i + step].to_numpy()])[1:-1]
        print(s_val)
        cur.execute(insert_ual.replace('{ual_val}',s_val))
        conn.commit()
        i += step+1
    
    conn.commit()
    cur.close()
    conn.close()
    return 200
    

#объявление DAG
dag = DAG(
    dag_id='v1',
    schedule_interval='0 0 * * *',
    start_date=datetime.now(),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60)
)


t_file_request = PythonOperator(
    task_id='file_request',
    python_callable=_create_files_request,
    op_kwargs={
        'base_url': base_url,
        'headers': headers
    },
    do_xcom_push=True,
    dag=dag
)

t_check_report = PythonOperator(
    task_id='check_report',
    python_callable=check_report,
    op_kwargs={  
        'base_url': base_url,
        'headers': headers
    },
    do_xcom_push=True,
    dag=dag
)

t_get_increment = PythonOperator(
    task_id='get_increment',
    python_callable=get_increment,
    op_kwargs={
        'date': business_dt,
        'base_url':base_url,
        'headers':headers
    },
    dag=dag
)

t_create_user_activity_log = PostgresOperator(
    task_id='create_user_activity_log',
    postgres_conn_id=postgres_conn,
    sql="sql/staging.create_user_activity_log.sql"  
)

t_create_user_order_log = PostgresOperator(
    task_id='create_user_order_log',
    postgres_conn_id=postgres_conn,
    sql="sql/staging.create_user_order_log.sql"  
)

t_create_customer_research = PostgresOperator(
    task_id='create_customer_research',
    postgres_conn_id=postgres_conn,
    sql="sql/staging.create_customer_research.sql"  
)

t_create_stage_inc_tables = PostgresOperator(
    task_id='create_stage_inc_tables',
    postgres_conn_id=postgres_conn,
    sql="sql/create_stage_inc_tables.sql"  
)


t_upload_from_s3_to_pg = PythonOperator(
    task_id='upload_from_s3_to_pg',
    python_callable=upload_from_s3_to_pg,
    op_kwargs={
        'NICKNAME': NICKNAME,
        'COHORT': COHORT,
        'date':business_dt
    },
    dag=dag
)

t_upload_inc_from_s3_to_pg = PythonOperator(
    task_id='upload_inc_from_s3_to_pg',
    python_callable=upload_inc_from_s3_to_pg,
    op_kwargs={
        'NICKNAME': NICKNAME,
        'COHORT': COHORT,
        'date':business_dt
    },
    dag=dag
)

t_update_mart_d_tables = PostgresOperator(
    task_id='update_mart_d_tables',
    postgres_conn_id=postgres_conn,
    sql="sql/update_mart_d_tables.sql"  
)

t_update_mart_f_tables = PostgresOperator(
    task_id='update_mart_f_tables',
    postgres_conn_id=postgres_conn,
    sql="sql/update_mart_f_table.sql"  
)

t_load_inc_d_tables = PostgresOperator(
    task_id='load_inc_d_tables',
    postgres_conn_id=postgres_conn,
    sql="sql/update_mart_d_tables.sql"  
)

t_load_inc_f_table = PostgresOperator(
    task_id='load_inc_f_table',
    postgres_conn_id=postgres_conn,
    sql="sql/update_mart_f_table.sql"  
)

t_creating_retention_mart = PostgresOperator(
    task_id='creating_retention_mart',
    postgres_conn_id=postgres_conn,
    sql="sql/creating_retention.sql"  
)

t_date_periods = PostgresOperator(
    task_id='date_periods',
    postgres_conn_id=postgres_conn,
    sql="sql/date_periods.sql"  
)

t_new_customers = PostgresOperator(
    task_id='new_customers',
    postgres_conn_id=postgres_conn,
    sql="sql/new_customers.sql"  
)

t_returned_customers = PostgresOperator(
    task_id='returned_customers',
    postgres_conn_id=postgres_conn,
    sql="sql/returned_customers.sql"  
)

t_refunded_customers = PostgresOperator(
    task_id='refunded_customers',
    postgres_conn_id=postgres_conn,
    sql="sql/refunded_customers.sql"  
)

t_loading_retention_mart = PostgresOperator(
    task_id='loading_retention_mart',
    postgres_conn_id=postgres_conn,
    sql="sql/loading_retention_mart.sql"  
)

t_file_request >> t_check_report >> t_get_increment >> [t_create_user_activity_log, t_create_user_order_log, 
t_create_customer_research] >> t_upload_from_s3_to_pg >> t_create_stage_inc_tables >> t_upload_inc_from_s3_to_pg >> t_update_mart_d_tables >> t_update_mart_f_tables >> t_load_inc_d_tables >> t_load_inc_f_table >> [t_creating_retention_mart,
t_date_periods] >> t_new_customers >> t_returned_customers >> t_refunded_customers >> t_loading_retention_mart
