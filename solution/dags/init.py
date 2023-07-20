<<<<<<< HEAD
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

def download_csv(
        ti: TaskInstance,
        file_name: str,
        start_at: str,
        symbol: str,
        interval: str,
):
    key = Variable.get(API_KEY_ID)

    response = StocksIntraDayExtendedHook(
        API_CONN_ID,
        symbol,
        interval,
        key,
    ).get_time_series()

    local_file_name = start_at.replace('-', '') + '_' + file_name
    path = 'dags/data/'+local_file_name
    open(path, 'wb').write(response.content)

    ti.xcom_push(key='file_path', value=path)
    ti.xcom_push(key='start_at', value=start_at)


def upload_csv(
        task: BaseOperator,
        ti: TaskInstance,
        symbol: str,
        interval: str,
        schema: str,

):
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
            cur.execute(f"select upload_id from {schema}.upload_hist where date='{start_at}'")
            upload_id = cur.fetchone()
            if upload_id and upload_id[0]:
                raise Exception(f"Запись за {start_at} уже существует!")

            cur.execute(f"insert into {schema}.upload_hist(symbol_name,interval_name,date) values('{symbol}', '{interval}', '{start_at}') returning upload_id;")
            upload_id = cur.fetchone()[0]
            df['upload_id'] = upload_id
            cols = ','.join(list(df.columns))

            if df.shape[0] > 0:
                insert_cr = f"INSERT INTO {schema}.stocks({cols}) VALUES " + "{cr_val};"
                i = 0
                step = int(df.shape[0] / 100)
                logging.info(f"insert stocks, step-{step}")

                while i <= df.shape[0]:

                    cr_val = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
                    cur.execute(insert_cr.replace('{cr_val}', cr_val))
                    conn.commit()

                    i += step + 1


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
            'symbol': 'IBM',
            'interval': INTERVAL,
        },
        provide_context=True,
    )

    t_upload_csv_from_api = PythonOperator(
        task_id='t_upload_csv_from_api',
        python_callable=upload_csv,
        provide_context=True,
        op_kwargs={
            'schema': SCHEMA_STAGE,
            'symbol': 'IBM',
            'interval': INTERVAL,
        },

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


    start >> t_download_csv_from_api >> t_upload_csv_from_api >> group_uploads >> end
=======
"""Даг инициализирующей загрузки."""
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator, TaskInstance, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from alphavantage_plugins.alphavantage_hooks import StocksIntraDayExtendedHook


API_KEY_ID = 'api_key'
API_CONN_ID = 'api_alphavantage'
PG_CONN_ID = 'postgres-db'

INTERVAL = '5min'
BATCH_SIZE = 100

SCHEMA_STAGE = 'staging'
SCHEMA_CORE = 'mart'

dt = '{{ ds }}'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}


def download_csv(
    ti: TaskInstance,
    file_name: str,
    start_at: str,
    symbol: str,
    interval: str,
):
    """Функция загрузки на локально хранение CSV файлов с данными."""
    def xcom_push():
        # Сохраняем название файла и дату выгрузки в XCOM для след-го таска.
        ti.xcom_push(key='file_path', value=path)
        ti.xcom_push(key='start_at', value=start_at)

    # Задаём путь до файла и сохраняем данные.
    local_file_name = start_at.replace('-', '') + '_' + file_name
    path = 'dags/data/'+local_file_name

    # Если файл уже существует, то пропускаем этот таск.
    if Path(path).exists():
        xcom_push()
        raise AirflowSkipException(f"Файл {path} уже скачан!")

    key = Variable.get(API_KEY_ID)

    response = StocksIntraDayExtendedHook(
        API_CONN_ID,
        symbol,
        interval,
        key,
    ).get_time_series()

    open(path, 'wb').write(response.content)
    xcom_push()


def upload_csv(
    task: BaseOperator,
    ti: TaskInstance,
    symbol: str,
    interval: str,
    schema: str,
):
    """Функция загрузки файлов данных в БД."""

    # Забираем из XCOM путь до файла и дату выгрузки.
    _id = task.upstream_list[0].task_id
    file_path = ti.xcom_pull(key='file_path', task_ids=[_id])[0]
    start_at = ti.xcom_pull(key='start_at', task_ids=[_id])[0]

    logging.info(f"Upload csv {file_path} on {start_at}")
    df = pd.read_csv(file_path, dtype=str)

    # Укажем time zone и дата автоматом конвертируется в UTC в БД
    df['time'] = df.apply(
        lambda d: str(pendulum.from_format(str(d['time']), 'YYYY-MM-DD HH:mm:ss', tz='US/Eastern')),
        axis=1)

    # Загружаем данные пачками в БД.
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:

            # Проверяем, что данные ранее не загружались.
            cur.execute(f"select upload_id from {schema}.upload_hist where symbol_name='{symbol}' and date='{start_at}' and uploaded is null")
            upload_id = cur.fetchone()
            if upload_id and upload_id[0]:
                raise AirflowSkipException(f"Запись за {start_at} уже существует!")

            # Сохраняем, что и когда загружаем, id выгрузки сохраним.
            cur.execute(f"insert into {schema}.upload_hist(symbol_name,interval_name,date) values('{symbol}', '{interval}', '{start_at}') returning upload_id;")
            upload_id = cur.fetchone()[0]
            df['upload_id'] = upload_id

            # Генерим название столбцов, для загрузки.
            # Название столбцов в CSV должны совпадать с названием в БД.
            cols = ','.join(list(df.columns))

            # Если Dataframe не пустой, то грузим его и логируем процесс.
            if df.shape[0] > 0:
                insert_cr = f"INSERT INTO {schema}.stocks({cols}) VALUES " + "{cr_val};"

                i = 0
                count = df.shape[0]
                step = int(count / BATCH_SIZE)

                while i <= count:

                    cr_val = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
                    cur.execute(insert_cr.replace('{cr_val}', cr_val))
                    conn.commit()

                    i += step + 1
                    logging.info(f"Loaded stocks time series {i} of {count}")


with DAG(
    'init-load',
    default_args=args,
    description='Initialize dag for symbols',
    start_date=datetime.today(),
    schedule_interval='@once',
) as dag:
    start = DummyOperator(task_id='start')

    # Таска, для того чтобы продолжить процесс, даже если только одна из групп исполнилась.
    checked = DummyOperator(task_id='checked', trigger_rule=TriggerRule.ONE_SUCCESS)
    end = DummyOperator(task_id='end')

    # Создадим группы скачивания и загрузки в БД, для каждого символа.
    stocks_tasks = list()
    for symbol in ['IBM', 'MSTR', 'META', 'BLUE', 'AAPL']:
        with TaskGroup(f"{symbol}_group_api_uploads") as group_api_uploads:
            t_download_csv_from_api = PythonOperator(
                task_id=f"{symbol}_download_csv_from_api",
                python_callable=download_csv,
                op_kwargs={
                    'file_name': f"{symbol}-init.csv",
                    'start_at': dt,
                    'symbol': symbol,
                    'interval': INTERVAL,
                },
                provide_context=True,
            )

            t_upload_to_staging = PythonOperator(
                task_id=f"{symbol}_upload_to_staging",
                python_callable=upload_csv,
                trigger_rule=TriggerRule.NONE_FAILED,
                provide_context=True,
                op_kwargs={
                    'schema': SCHEMA_STAGE,
                    'symbol': symbol,
                    'interval': INTERVAL,
                },
            )

            t_download_csv_from_api >> t_upload_to_staging

        stocks_tasks.append(group_api_uploads)

    # Создадим группу, для переноса данных из staging в core.
    with TaskGroup('group_update_core') as group_update_core:
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
                parameters={
                    'date': {dt}
                },

            ))
        dimension_tasks[0] >> dimension_tasks[2] >> dimension_tasks[1] >> dimension_tasks[3] >> dimension_tasks[4]

    start >> stocks_tasks >> checked >> group_update_core >> end
>>>>>>> afb12017938b5a5f5ca5369e3bc6953190f6cf9b
