"""

"""
import json
import pendulum
import logging
import time
from dataclasses import asdict
import pandas as pd
import requests
from model import Stock
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from alphavantage_plugins.alphavantage_hooks import StocksIntraDayHook

API_KEY_ID = 'api_key'
API_CONN_ID = 'api_alphavantage'

INTERVAL = '5min'

PG_CONN_ID = 'postgres-db'

SCHEMA_STAGE = 'staging'
SCHEMA_CORE = 'mart'

BATCH_SIZE = 100

dt = '{{ ds }}'
date_last_success = '{{ prev_start_date_success }}'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

def download_json(ti: TaskInstance, file_name: str, start_at: str, symbol: str, interval: str):
    key = Variable.get(API_KEY_ID)

    response = StocksIntraDayHook(
        API_CONN_ID,
        symbol,
        interval,
        key,
    ).get_time_series()


    local_file_name = start_at.replace('-', '') + '_' + file_name
    path = 'dags/data/'+local_file_name
    file = open(path, 'wt', encoding='utf-8')
    json.dump(response, file)


    ti.xcom_push(key='file_path', value=path)
    ti.xcom_push(key='start_at', value=start_at)


def upload_json(task: BaseOperator, ti: TaskInstance, symbol: str, schema: str, interval: str):
    _id = task.upstream_list[0].task_id
    file_path = ti.xcom_pull(key='file_path', task_ids=[_id])[0]
    start_at = ti.xcom_pull(key='start_at', task_ids=[_id])[0]

    file = open(file_path, 'rt', encoding='utf-8')
    json_file = json.load(file)

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"select upload_id from {schema}.upload_hist where symbol_name='{symbol}' and date='{start_at}' and uploaded is true")
            upload_id = cur.fetchone()
            if upload_id and upload_id[0]:
                raise AirflowSkipException(f"Запись за {start_at} уже существует!")
            cur.execute(f"insert into {schema}.upload_hist(symbol_name,interval_name,date) values('{symbol}', '{interval}', '{start_at}') returning upload_id;")
            upload_id = cur.fetchone()[0]

            cur.execute(f"select column_name from information_schema.columns where table_name='stocks' and table_schema='{schema}'")
            colls = cur.fetchall()
            collstr = ''

            stock_keys = Stock.__annotations__.keys()

            for col in colls:
                if col[0] in stock_keys:
                    collstr += ','.join(col) + ','
                else:
                    logging.warning(f"ID {col} not founded for model!")

            collstr = collstr[:-1]

            count=len(json_file['Time Series (5min)'].items())
            logging.info(f"Count elements in json is {count}")
            i = 1
            step = int(count / BATCH_SIZE)
            logging.info(f"insert stocks, steps is {step}")

            insert_cr = f"INSERT INTO {schema}.stocks({collstr}) VALUES " + "{cur_val};"
            vals = []

            for el in json_file['Time Series (5min)'].items():

                stock: Stock = Stock(
                    time=el[0],
                    open=el[1]['1. open'],
                    high=el[1]['2. high'],
                    low=el[1]['3. low'],
                    close=el[1]['4. close'],
                    volume=el[1]['5. volume'],
                    upload_id=str(upload_id),
                )
                stock_dict = asdict(stock)
                val = []
                for col in colls:
                    try:
                        val.append(stock_dict[col[0]])
                    except KeyError:
                        continue

                vals.append(val)
                length = len(vals)
                if  length >= 100 or length + (i * BATCH_SIZE) == count:
                    cur_val = str([tuple(x) for x in vals])[1:-1]
                    cur.execute(insert_cr.replace('{cur_val}', cur_val))
                    conn.commit()
                    vals=[]
                    logging.info(f"insert stocks, step {i} from {step}")
                    i += 1

with DAG(
        'increment-load-STOCKS',
        default_args=args,
        description='increment dag for load symbol',
        start_date=datetime.today(),
        schedule_interval='@once',
) as dag:
    start = DummyOperator(task_id='start')
    checked = DummyOperator(task_id='checked', trigger_rule=TriggerRule.ONE_SUCCESS)
    end = DummyOperator(task_id='end')

    stocks_tasks = list()
    for symbol in ['IBM', 'MSTR', 'META']:
        with TaskGroup(f"{symbol}_group_downloads") as group_downloads:
            t_download_from_api = PythonOperator(
                task_id=f"{symbol}_download_from_api",
                python_callable=download_json,
                op_kwargs={
                    'file_name': f"stocks.json",
                    'start_at': dt,
                    'symbol': symbol,
                    'interval': INTERVAL,
                },
                provide_context=True,
            )

            t_upload_json = PythonOperator(
                task_id=f"{symbol}_upload_json",
                python_callable=upload_json,
                op_kwargs={
                    'schema': SCHEMA_STAGE,
                    'symbol': symbol,
                    'interval': INTERVAL,
                },

                provide_context=True,
            )

            t_download_from_api >> t_upload_json

        stocks_tasks.append(group_downloads)

    with TaskGroup('group_uploads') as group_uploads:
        dimension_tasks = list()
        for i in [
            '0-dml-d_interval',
            '1-dml-d_time_serial',
            '2-dml-d_symbol_act',
            '3-dml-d_symbol_hist',
            '4-dml-f_price',
        ]:
            dimension_tasks.append(SQLExecuteQueryOperator(
                task_id=f'update_{i}',
                conn_id=PG_CONN_ID,
                sql=f"sql/{i}.sql",
                dag=dag,
                parameters={'date': {dt}},

            ))

        t_uploaded_fixing = SQLExecuteQueryOperator(
            task_id='t_uploaded_fixing',
            conn_id=PG_CONN_ID,
            sql="""
                update staging.upload_hist set uploaded=true  where date='{{ds}}';
            """,
            dag=dag
        )

        dimension_tasks[0] >> dimension_tasks[2] >> dimension_tasks[1] >> dimension_tasks[3] >> dimension_tasks[4] >> t_uploaded_fixing


    start >> stocks_tasks >> checked >> group_uploads >> end
