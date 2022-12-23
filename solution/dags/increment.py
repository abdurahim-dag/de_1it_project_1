"""

"""
import json
import pendulum
import logging
import time

import pandas as pd
import requests

from datetime import datetime

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

from airflow.models import Variable

from alphavantage_plugins.alphavantage_hooks import StocksIntraDayExtendedHook

API_KEY_ID = 'api_key'
API_CONN_ID = 'api_alphavantage'

SYMBOL = 'IBM'
INTERVAL = '5min'

PG_CONN_ID = 'postgres-db'

SCHEMA_STAGE = 'staging'
SCHEMA_CORE = 'mart'

dt = '{{ ds }}'
date_last_success = '{{ prev_start_date_success }}'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

def download_json(ti: TaskInstance, file_name: str, start_at: str):
    key = Variable.get(API_KEY_ID)

    response = StocksIntraDayExtendedHook(
        API_CONN_ID,
        SYMBOL,
        INTERVAL,
        key,
    ).get_time_series()

    local_file_name = start_at.replace('-', '') + '_' + file_name
    path = 'dags/data/'+local_file_name
    open(path, 'wb').write(response.content)

    ti.xcom_push(key='file_path', value=path)
    ti.xcom_push(key='start_at', value=start_at)


def upload_json(task: BaseOperator, ti: TaskInstance):
    _id = task.upstream_list[0].task_id
    file_path = ti.xcom_pull(key='file_path', task_ids=[_id])[0]
    start_at = ti.xcom_pull(key='start_at', task_ids=[_id])[0]
    df = pd.read_csv(file_path, dtype=str)

    # Укажем time zone и дата автоматом конвертируется в UTC в БД
    df['time'] = df.apply(
        lambda d: str(pendulum.from_format(str(d['time']), 'YYYY-MM-DD HH:mm:ss', tz='US/Eastern')),
        axis=1)

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"select upload_id from {SCHEMA_STAGE}.upload_hist where date='{start_at}'")
            upload_id = cur.fetchone()
            logging.info("upload_id")
            logging.info(upload_id)
            if upload_id and upload_id[0]:
                raise Exception(f"Запись за {start_at} уже существует!")

            cur.execute(f"insert into {SCHEMA_STAGE}.upload_hist(symbol_name,interval_name,date) values('{SYMBOL}', '{INTERVAL}', '{start_at}') returning upload_id;")
            upload_id = cur.fetchone()[0]
            df['upload_id'] = upload_id
            cols = ','.join(list(df.columns))

            if df.shape[0] > 0:
                insert_cr = f"INSERT INTO {SCHEMA_STAGE}.stocks({cols}) VALUES " + "{cr_val};"
                i = 0
                step = int(df.shape[0] / 100)
                logging.info(f"insert stocks, step-{step}")

                while i <= df.shape[0]:

                    cr_val = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
                    cur.execute(insert_cr.replace('{cr_val}', cr_val))
                    conn.commit()

                    i += step + 1

# DAG#
with DAG(
        'init-load-IBM',
        default_args=args,
        description='Initialize dag for symbol IBM',
        start_date=datetime.today(),
        schedule_interval='@once',
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    t_download_csv_from_api = PythonOperator(
        task_id='t_download_csv_from_api',
        python_callable=download_csv,
        op_kwargs={
            'file_name': 'init.csv',
            'start_at': dt,
        },
        provide_context=True,
    )

    t_upload_csv_from_api = PythonOperator(
        task_id='t_upload_csv_from_api',
        python_callable=upload_csv,
        provide_context=True,
    )

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
        dimension_tasks[0] >> dimension_tasks[2] >> dimension_tasks[1] >> dimension_tasks[3] >> dimension_tasks[4]

    t_uploaded_fixing = SQLExecuteQueryOperator(
        task_id='t_uploaded_fixing',
        conn_id=PG_CONN_ID,
        sql="""
            update staging.upload_hist set uploaded=true  where date='{{ds}}'; 
        """,
        dag=dag
    )


    start >> t_download_csv_from_api >> t_upload_csv_from_api >> group_uploads >> t_uploaded_fixing >> end