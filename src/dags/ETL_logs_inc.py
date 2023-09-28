from datetime import datetime, timedelta
from re import T
import time
import psycopg2

import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models.xcom import XCom

# set user constants for api
nickname = 'Matthew'
cohort = '18'
http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

headers = {
    "X-API-KEY": api_key,
    "X-Nickname": nickname,
    "X-Cohort": cohort,
    'X-Project': 'True',
    'Content-Type': 'application/x-www-form-urlencoded'
}

# business_dt = '{{ ds }}'
date = datetime.today() 
one_day = timedelta(days=1)  
business_dt = date - one_day 
business_dt = business_dt.strftime("%Y-%m-%d")
# business_dt = datetime.now().strftime("%Y-%m-%d")


### POSTGRESQL settings ###
# set postgresql connectionfrom basehook
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
    # task_id = 'MjAyMy0wOS0yN1QwNzo1NjoyOAlNYXR0aGV3'
    # print(f"task_id is {response_dict['task_id']}")
    # return response_dict['task_id']
    print(f'Response is {response.content}')


# проверка готовности файлов 
def check_report(ti, base_url, headers):
    task_ids = ti.xcom_pull(key='task_id')
    task_id = task_ids
    
    method_url = '/get_report'
    payload = {'task_id': task_id}

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)


    if not report_id:
        raise TimeoutError()

    # for i in range(4):
    #     time.sleep(70)
    #     r = requests.get('https://' + api_endpoint + method_url, params=payload, headers=headers)
    #     response_dict = json.loads(r.content)
    #     print(i, response_dict['status'])
    #     if response_dict['status'] == 'SUCCESS':
    #         report_id = response_dict['data']['report_id']
    #         break
    # report_id = 'TWpBeU15MHdPUzB5TjFRd056bzFOam95T0FsTllYUjBhR1Yz'
    
    # если report_id не объявлен и success не было, то здесь возникнет ошибка
    ti.xcom_push(key='report_id', value=report_id)
    print(f"Report_id is {report_id}")

def get_increment(date, ti, base_url, headers):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
        # print('Increment is empty. Most probably due to error in API call.')
        # then replace increment_id with report_id
        # increment_id = report_id
    
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')

def create_stage_tables(ti):
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    sql = '''
    drop table if exists staging.user_activity_log;
    CREATE TABLE if not exists staging.user_activity_log(
        ID serial ,
        uniq_id varchar(255),
        date_time          TIMESTAMP ,
        action_id             BIGINT ,
        customer_id             BIGINT ,
        quantity             BIGINT ,
        PRIMARY KEY (ID)
    );
    ''' 
    cur.execute(sql)
    sql = '''
    drop table if exists staging.user_order_log;
    CREATE TABLE if not exists staging.user_order_log(
        ID serial,
        uniq_id text,
        date_time          TIMESTAMP,
        city_id integer,
        city_name varchar(100),
        customer_id             BIGINT ,
        first_name varchar(100),
        last_name varchar(100),
        item_id integer,
        item_name varchar(100),
        quantity             BIGINT ,
        payment_amount numeric(14,2),
        PRIMARY KEY (ID)
    );
    '''
    cur.execute(sql)
    sql = '''
    drop table if exists staging.customer_research;
    CREATE TABLE if not exists staging.customer_research(
        ID serial ,
        date_id          TIMESTAMP ,
        category_id             integer ,
        geo_id             integer ,
        sales_qty             integer ,
        sales_amt numeric(14,2),
        PRIMARY KEY (ID)
    );
    '''
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

    return 200


#загружаем файлы в таблицы stage
def upload_cr_from_s3_to_pg(ti,nickname,cohort, date):
    report_id = ti.xcom_pull(key='report_id')
    #insert to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #get custom_research
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{str(report_id)}/customer_research.csv'
    local_filename = f"{date.replace('-', '')}_customer_research.csv"
    response = requests.get(s3_filename)
    response.raise_for_status()
    print(local_filename)
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
    cur.close()
    conn.close()
    return 200
    
def upload_pl_from_s3_to_pg(ti,nickname,cohort,date):
    report_id = ti.xcom_pull(key='report_id')

    # insert to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # get order log
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{str(report_id)}/price_log.csv'
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

    cur.close()
    conn.close()
    return 200

def upload_ual_from_s3_to_pg(ti,nickname,cohort,date):
    report_id = ti.xcom_pull(key='report_id')

    # insert to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # get order log
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{str(report_id)}/user_activity_log.csv'
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

    cur.close()
    conn.close()
    return 200

def upload_uol_from_s3_to_pg(ti,nickname,cohort,date):
    report_id = ti.xcom_pull(key='report_id')

    # insert to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # get order log
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{str(report_id)}/user_order_log.csv'
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

#обновление таблиц d по загруженным в staging-слой данным
def update_mart_d_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #d_calendar
    cur.execute(
        '''
        drop sequence if exists date_id_seq;
        create sequence date_id_seq start 1;
        truncate table mart.d_calendar cascade;
        insert into mart.d_calendar 
        with all_dates as (
            select distinct(date_time::timestamp) as fact_date from (
                select date_id as date_time from staging.customer_research cr
                    union all 
                select date_time from staging.user_activity_log ual
                    union all 
                select date_time from staging.user_order_log uol
            ) as dates
        )
        select nextval('date_id_seq') as date_id , fact_date, extract(day from fact_date) as day_num, extract(month from fact_date) as month_num, 
        TO_CHAR(fact_date, 'mon') as month_name, extract(year from fact_date) as year_num from all_dates;
        '''
    )
    conn.commit()
    cur.execute(
        '''
        truncate table mart.f_sales cascade;
        '''
    )
    conn.commit()

    #d_customer
    cur.execute(
        '''
        delete from mart.d_customer;
        drop sequence if exists d_customer_id_seq;
        create sequence d_customer_id_seq start 1;
        insert into mart.d_customer 
        select distinct on (customer_id) nextval('d_customer_id_seq') as id, cast(customer_id as int4) as customer_id, first_name, last_name, max(city_id) as city_id from staging.user_order_log uol group by 1,2,3,4;
        '''
    )
    conn.commit()


    #d_item
    cur.execute(
        '''
        delete from mart.d_item;
        drop sequence if exists d_item_id_seq;
        create sequence d_item_id_seq start 1;
        insert into mart.d_item
        select distinct on (item_id) nextval('d_item_id_seq'), item_id::int4, item_name from staging.user_order_log;
        '''
    )

    conn.commit()

    cur.close()
    conn.close()

    return 200


#обновление витрин (таблицы f)
def update_mart_f_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #f_activity
    cur.execute(
        '''
        truncate table mart.f_sales cascade;
        drop sequence if exists f_sales_id_seq;
        create sequence f_sales_id_seq start 1;
        insert into mart.f_sales 
        select nextval('f_sales_id_seq') as id, dc.date_id as date_id, uol.item_id as item_id, uol.customer_id as customer_id, uol.city_id as city_id, uol.quantity as quantity, uol.payment_amount as payment_amount
        from staging.user_order_log uol join mart.d_calendar dc on cast(dc.fact_date as timestamp)=cast(uol.date_time as timestamp);  
        '''
    )
    conn.commit()


    #f_daily_sales
    cur.execute(
        '''
        drop table if exists mart.f_sales_v2;
        create table mart.f_sales_v2 as
        select * from mart.f_sales;

        alter table mart.f_sales_v2 add column status varchar(10);        
        '''
    )
    conn.commit()

    cur.close()
    conn.close()


    return 200

def create_stage_inc_tables(ti):
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    sql = '''
    drop table if exists staging.user_activity_log_temp;
    CREATE TABLE if not exists staging.user_activity_log_temp(
        ID serial ,
        uniq_id varchar(255),
        date_time          TIMESTAMP ,
        action_id             BIGINT ,
        customer_id             BIGINT ,
        quantity             BIGINT ,
        PRIMARY KEY (ID)
    );
    ''' 
    cur.execute(sql)
    sql = '''
    drop table if exists staging.user_order_log_temp;
    CREATE TABLE if not exists staging.user_order_log_temp(
        ID serial,
        uniq_id text,
        date_time          TIMESTAMP,
        city_id integer,
        city_name varchar(100),
        customer_id             BIGINT ,
        first_name varchar(100),
        last_name varchar(100),
        item_id integer,
        item_name varchar(100),
        quantity             BIGINT ,
        payment_amount numeric(14,2),
        status varchar(10),
        PRIMARY KEY (ID)
    );
    '''
    cur.execute(sql)
    sql = '''
    drop table if exists staging.customer_research_temp;
    CREATE TABLE if not exists staging.customer_research_temp(
        ID serial ,
        date_id          TIMESTAMP ,
        category_id             integer ,
        geo_id             integer ,
        sales_qty             integer ,
        sales_amt numeric(14,2),
        PRIMARY KEY (ID)
    );
    '''
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

    return 200

#загружаем файлы в таблицы stage
def upload_cr_inc_from_s3_to_pg(ti,nickname,cohort, date):
    increment_id = ti.xcom_pull(key='increment_id')

    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{str(increment_id)}/customer_research_inc.csv'
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
    
    conn.commit()
    cur.close()
    conn.close()
    return 200
    

def upload_uol_inc_from_s3_to_pg(ti,nickname,cohort, date):
    increment_id = ti.xcom_pull(key='increment_id')

    # insert to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    # get order log
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{str(increment_id)}/user_order_log_inc.csv'
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
    conn.commit()
    cur.close()
    conn.close()
    return 200

def upload_ual_inc_from_s3_to_pg(ti,nickname,cohort, date):
    increment_id = ti.xcom_pull(key='increment_id')

    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{str(increment_id)}/user_activity_log_inc.csv'
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


def load_inc_d_tables(ti):
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    sql = '''
        drop table if exists mart.d_calendar_temp cascade;
        create table mart.d_calendar_temp as
            with all_dates as (
            select distinct(date_time::timestamp) as fact_date from (
                select date_id as date_time from staging.customer_research_temp cr
                    union all
                select date_time from staging.user_activity_log_temp ual
                    union all
                select date_time from staging.user_order_log_temp uol
            ) as dates
            )
        select nextval('date_id_seq') as date_id , fact_date, extract(day from fact_date) as day_num, extract(month from fact_date) as month_num,
        TO_CHAR(fact_date, 'mon') as month_name, extract(year from fact_date) as year_num from all_dates;
        drop sequence date_id_seq;
    ''' 
    cur.execute(sql)
    sql = '''
        drop table mart.d_item_temp cascade;
        create table mart.d_item_temp as
        select distinct on (item_id) nextval('d_item_id_seq'), item_id::int4, item_name from staging.user_order_log_temp;
        drop sequence d_item_id_seq;
    '''
    cur.execute(sql)
    sql = '''
        drop table mart.d_customer_temp cascade;
        create table mart.d_customer_temp as
        select distinct on (customer_id) nextval('d_customer_id_seq'), cast(customer_id as int4), first_name, last_name, max(city_id) from staging.user_order_log_temp uol group by 1,2,3,4;
        drop sequence d_customer_id_seq;
    '''
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

    return 200

def load_inc_f_table(ti):
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    sql = '''
        drop table if exists mart.f_sales_temp;
        create table mart.f_sales_temp as
        select nextval('f_sales_id_seq') as id, dc.date_id as date_id, uol.item_id as item_id, uol.customer_id as customer_id, uol.city_id as city_id, uol.quantity as quantity, uol.payment_amount as payment_amount, uol.status as status
        from staging.user_order_log_temp uol join mart.d_calendar_temp dc on cast(dc.fact_date as timestamp)=cast(uol.date_time as timestamp);
        drop sequence f_sales_id_seq;

        update mart.f_sales_v2 set status='shipped';

        insert into mart.f_sales_v2 
        select * from mart.f_sales_temp; 
    ''' 
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    return 200

def creating_retention_mart(ti):
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    sql = '''
drop table if exists mart.f_customer_retention;
create table mart.f_customer_retention(
    new_customers_count int4,
    returning_customers_count int4,
    refunded_customer_count int4,
    period_name varchar default 'weekly',
    period_id int4,
    item_id_new_cust int4,
    item_id_returned_cust int4,
    item_id_refunded_cust int4,
    new_customers_revenue bigint,
    returning_customers_revenue bigint,
    customers_refunded int4
);


insert into mart.d_calendar
select * from mart.d_calendar_temp;

select fact_date, extract(dow from fact_date) from mart.d_calendar dc order by 1 ;

drop table if exists mart.d_calendar_weeks;
create table mart.d_calendar_weeks as
SELECT
    MIN(fact_date) AS start_date,
    MAX(fact_date) AS end_date,
    ROW_NUMBER() OVER (ORDER BY MIN(fact_date)) AS period
FROM
    mart.d_calendar
GROUP BY
    DATE_TRUNC('week', fact_date)
ORDER BY
    start_date;

drop table if exists staging.user_order_log_v2;
create table staging.user_order_log_v2 as
select * from staging.user_order_log uol;

alter table staging.user_order_log_v2 add column status varchar(10);

update staging.user_order_log_v2 set status='shipped';

insert into staging.user_order_log_v2 
select * from staging.user_order_log_temp;


drop table if exists mart.new_cust;
create table mart.new_cust as
with new_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date), item_id,
   count(*), sum(payment_amount) as revenue from staging.user_order_log_v2 uolv where status='shipped' group by 1,2,3,4 having count(*)=1
)
select period, item_id as item_new, count(customer_id) as new_cust_count, 
sum(revenue) as new_cust_revenue from new_cust nc2 group by 1,2;

drop table if exists mart.new_cust_compact;
create table mart.new_cust_compact as
with new_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date),
   count(*), sum(payment_amount) as revenue from staging.user_order_log_v2 uolv where status='shipped' group by 1,2,3 having count(*)=1
)
select period, count(customer_id) as new_cust_count, 
sum(revenue) as new_cust_revenue from new_cust nc2 group by 1;

drop table if exists mart.returned_cust;
create table mart.returned_cust as
with returned_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date), item_id,
   count(*), sum(payment_amount) as revenue from staging.user_order_log_v2 uolv where status='shipped' group by 1,2,3,4 having count(*)>1
)
select period, item_id as item_ret, count(customer_id) as returned_cust_count, 
sum(revenue) as returned_revenue from returned_cust rc2 group by 1,2;

drop table if exists mart.returned_cust_compact;
create table mart.returned_cust_compact as
with returned_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date),
   count(*), sum(payment_amount) as revenue from staging.user_order_log_v2 uolv where status='shipped' group by 1,2,3 having count(*)>1
)
select period, count(customer_id) as returned_cust_count, 
sum(revenue) as returned_revenue from returned_cust rc2 group by 1;

drop table if exists mart.refunded_cust;
create table mart.refunded_cust as
with refunded_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date), item_id,
   count(*) as refunds from staging.user_order_log_v2 uolv where status='refunded' group by 1,2,3,4
)
select period, item_id as item_ref, count(*) cust_with_refunds, sum(refunds) total_refunds from refunded_cust group by 1,2; -- refunded

drop table if exists mart.refunded_cust_compact;
create table mart.refunded_cust_compact as
with refunded_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date),
   count(*) as refunds from staging.user_order_log_v2 uolv where status='refunded' group by 1,2,3
)
select period, count(*) cust_with_refunds, sum(refunds) total_refunds from refunded_cust group by 1; -- refunded

drop table if exists mart.retention_compact; 
create table mart.retention_compact as
select * from mart.new_cust_compact full join mart.returned_cust_compact using(period) full join mart.refunded_cust_compact using(period) order by 1;


truncate table mart.f_customer_retention cascade;
insert into mart.f_customer_retention
select new_cust_count, returned_cust_count, cust_with_refunds, 'weekly', period, item_new, item_ret, item_ref, new_cust_revenue, returned_revenue, total_refunds 
from mart.new_cust full join mart.returned_cust using(period) full join mart.refunded_cust using(period) order by 1;

    ''' 
    cur.execute(sql)
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

t_create_stage_tables = PythonOperator(
    task_id='create_stage_tables',
    python_callable=create_stage_tables,
    do_xcom_push=False,
    dag=dag
)

t_create_stage_inc_tables = PythonOperator(
    task_id='create_stage_inc_tables',
    python_callable=create_stage_inc_tables,
    do_xcom_push=False,
    dag=dag
)

t_upload_cr_from_s3_to_pg = PythonOperator(
    task_id='upload_cr_from_s3_to_pg',
    python_callable=upload_cr_from_s3_to_pg,
    op_kwargs={
        'nickname': nickname,
        'cohort': cohort,
        'date':business_dt
    },
    dag=dag
)

t_upload_uol_from_s3_to_pg = PythonOperator(
    task_id='upload_uol_from_s3_to_pg',
    python_callable=upload_uol_from_s3_to_pg,
    op_kwargs={
        'nickname': nickname,
        'cohort': cohort,
        'date':business_dt
    },
    dag=dag
)

t_upload_ual_from_s3_to_pg = PythonOperator(
    task_id='upload_ual_from_s3_to_pg',
    python_callable=upload_ual_from_s3_to_pg,
    op_kwargs={
        'nickname': nickname,
        'cohort': cohort,
        'date':business_dt
    },
    dag=dag
)

t_upload_pl_from_s3_to_pg = PythonOperator(
    task_id='upload_pl_from_s3_to_pg',
    python_callable=upload_pl_from_s3_to_pg,
    op_kwargs={
        'nickname': nickname,
        'cohort': cohort,
        'date':business_dt
    },
    dag=dag
)

t_upload_cr_inc_from_s3_to_pg = PythonOperator(
    task_id='upload_cr_inc_from_s3_to_pg',
    python_callable=upload_cr_inc_from_s3_to_pg,
    op_kwargs={
        'nickname': nickname,
        'cohort': cohort,
        'date':business_dt
    },
    dag=dag
)

t_upload_uol_inc_from_s3_to_pg = PythonOperator(
    task_id='upload_uol_inc_from_s3_to_pg',
    python_callable=upload_uol_inc_from_s3_to_pg,
    op_kwargs={
        'nickname': nickname,
        'cohort': cohort,
        'date':business_dt
    },
    dag=dag
)

t_upload_ual_inc_from_s3_to_pg = PythonOperator(
    task_id='upload_ual_inc_from_s3_to_pg',
    python_callable=upload_ual_inc_from_s3_to_pg,
    op_kwargs={
        'nickname': nickname,
        'cohort': cohort,
        'date':business_dt
    },
    dag=dag
)


t_update_mart_d_tables = PythonOperator(
    task_id='update_mart_d_tables',
    python_callable=update_mart_d_tables,
    dag=dag
)


t_update_mart_f_tables = PythonOperator(
    task_id='update_mart_f_tables',
    python_callable=update_mart_f_tables,
    dag=dag
)

t_load_inc_d_tables = PythonOperator(
    task_id='load_inc_d_tables',
    python_callable=load_inc_d_tables,
    do_xcom_push=False,
    dag=dag
)

t_load_inc_f_table = PythonOperator(
    task_id='load_inc_f_table',
    python_callable=load_inc_f_table,
    do_xcom_push=False,
    dag=dag
)

t_creating_retention_mart = PythonOperator(
    task_id='creating_retention_mart',
    python_callable=creating_retention_mart,
    do_xcom_push=False,
    dag=dag
)

t_file_request >> t_check_report >> t_get_increment >> t_create_stage_tables >> [t_upload_cr_from_s3_to_pg, t_upload_uol_from_s3_to_pg, 
t_upload_ual_from_s3_to_pg, t_upload_pl_from_s3_to_pg] >> t_create_stage_inc_tables >> [t_upload_cr_inc_from_s3_to_pg,
t_upload_uol_inc_from_s3_to_pg, t_upload_ual_inc_from_s3_to_pg] >> t_update_mart_d_tables >> t_update_mart_f_tables >> t_load_inc_d_tables >> t_load_inc_f_table >> t_creating_retention_mart