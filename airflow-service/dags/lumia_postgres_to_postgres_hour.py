import json
import logging
import os
import geonamescache
import pandas
from typing import Final
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine, text

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOG: Final[logging.Logger] = logging.getLogger(__name__)

LUMIA_SSH_CONN_ID: Final[str] = "lumia_ssh"
LUMIA_POSTGRES_CONN_ID: Final[str] = "lumia_postgres"
ANALYTIC_POSTGRES_CONN_ID: Final[str] = "analytic_postgres"
FASTAPI_CONN_ID: Final[str] = "fastapi_http"

BASE_DIR = Path(__file__).resolve().parent.parent
PATH_TO_RAW_DATA: Final[Path] = BASE_DIR / "raw_data"
PATH_TO_SQL_SCRIPTS: Final[Path] = BASE_DIR / "queries"
EXTRACT_SCRIPT: Final[Path] = "DML_select_data.sql"


def extract_data(ti, date: str, hour: str) -> None:
    pg_hook = PostgresHook(postgres_conn_id=LUMIA_POSTGRES_CONN_ID)
    conn = pg_hook.get_connection(LUMIA_POSTGRES_CONN_ID)
    ssh_hook = SSHHook(ssh_conn_id=LUMIA_SSH_CONN_ID)
    with ssh_hook.get_tunnel(remote_port=5432, remote_host="127.0.0.1") as tunnel:
        local_port = tunnel.local_bind_port
        LOG.info(f"Open SSH Tunnel on port: {local_port}")
        db_url = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}@"
            f"127.0.0.1:{local_port}/{conn.schema}"
        )
        engine = create_engine(db_url)
        
        sql_path = PATH_TO_SQL_SCRIPTS / EXTRACT_SCRIPT
        with open(sql_path, 'r') as f:
            query_raw = f.read().format(date_param=date, hour_param=hour)
        with engine.connect() as connection:
            result = connection.execute(text(query_raw))
            data_df = pandas.DataFrame(result.fetchall(), columns=result.keys())

        PATH_TO_RAW_DATA.mkdir(parents=True, exist_ok=True)
        file_name = PATH_TO_RAW_DATA / f"{date.replace('-', '')}_{hour}.csv"
        
        LOG.info(f"Writing {len(data_df)} rows to CSV: {file_name}")
        data_df.to_csv(file_name, index=False)

        ti.xcom_push(key="file_name", value=str(file_name))


def encriment_countries(ti) -> None:
    file_name = ti.xcom_pull(key="file_name")
    df = pandas.read_csv(file_name)
    if df.empty:
        LOG.info("File is empty")
        return

    cities_df = df["residence_city"].drop_duplicates()
    gc = geonamescache.GeonamesCache()
    cities = gc.get_cities()
    
    city_map = {}
    for city in cities.values():
        city_map[city['name']] = city['countrycode'] 
    data = []
    for city in cities_df:
        country = city_map.get(city, None)
        data.append({"city_key": city, "residence_country": country})
    countries_df = pandas.DataFrame(data)

    result_df = df.merge(
        countries_df, 
        left_on='residence_city',  
        right_on='city_key',       
        how='left'                
    )
    result_df = result_df.drop(columns=['city_key'])
    result_df.to_csv(file_name, index=False)


def load_data_to_staging(ti, pg_schema: str, pg_table: str) -> None:
    file_name = ti.xcom_pull(key="file_name")
    if not file_name:
        raise ValueError("File not found")

    df = pandas.read_csv(file_name)
    df = df.rename(columns={'user_id': 'tg_user_id'})
    target_fields = [
        'tg_user_id', 'sex', 'residence_city', 'residence_country',
        'registration_date', 'prod_str_id', 'prod_name', 'prod_category', 
        'transaction_date', 'transaction_time', 'stars_price_original',
        'stars_price_actual', 'is_subscription_active', 'refunded'
    ]
    existing_columns = [col for col in target_fields if col in df.columns]
    df = df[existing_columns]

    rows = [tuple(x) for x in df.to_numpy()]
    target_table = f"{pg_schema}.{pg_table}"
    pg_hook = PostgresHook(postgres_conn_id='analytic_postgres')
    try:
        LOG.info(f"Loading {len(rows)} rows into {target_table}")
        pg_hook.insert_rows(
            table=target_table,
            rows=rows,
            target_fields=existing_columns,
            commit_every=1000
        )
        LOG.info("Load finished successfully.")
    except Exception as e:
        raise RuntimeError(f"Error data load: {e}")
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
            LOG.info(f'Temp file deleted: {file_name}')


def check_maintenance(logical_date) -> str:
    if logical_date.weekday() == 6 and logical_date.hour == 0:
        return 'notify_api_start'
    return 'skip_maintenance'


args = {
    "owner": "analytics",
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
date = "{{ ds }}"
hour = "{{ logical_date.strftime('%H') }}"

with DAG(
    "lumia_postgres_to_postgres_hour.py",
    default_args=args,
    description="Lumia Analytic",
    catchup=True,
    max_active_runs=1,
    start_date=datetime(2026, 4, 14),
    end_date=datetime.now() - timedelta(hours=1),
    schedule="0 * * * *",
    template_searchpath=[BASE_DIR],
) as dag:

    start_etl_task = EmptyOperator(
        task_id="start_etl",
        dag=dag
    )

    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        op_kwargs={
            "date": date,
            "hour": hour
        }
    )

    encriment_countries_task = PythonOperator(
        task_id="encriment_countries",
        python_callable=encriment_countries,
        dag=dag
    )

    load_data_to_staging_task = PythonOperator(
        task_id="load_data_to_staging",
        python_callable=load_data_to_staging,
        op_kwargs={
            "pg_schema": "staging",
            "pg_table": "transaction"
        },
        dag=dag
    )

    ETL_to_migrations_task = EmptyOperator(
        task_id="ETL_to_migrations",
        dag=dag
    )

    update_d_calendar_task = SQLExecuteQueryOperator(
        task_id="update_d_calendar",
        conn_id=ANALYTIC_POSTGRES_CONN_ID,
        sql="queries/DML_d_calendar_update.sql",
        dag=dag
    )

    update_d_country_task = SQLExecuteQueryOperator(
        task_id="update_d_country",
        conn_id=ANALYTIC_POSTGRES_CONN_ID,
        sql="queries/DML_d_country_update.sql",
        dag=dag
    )

    update_d_city_task = SQLExecuteQueryOperator(
        task_id="update_d_city",
        conn_id=ANALYTIC_POSTGRES_CONN_ID,
        sql="queries/DML_d_city_update.sql",
        dag=dag
    )

    update_d_product_task = SQLExecuteQueryOperator(
        task_id="update_d_product",
        conn_id=ANALYTIC_POSTGRES_CONN_ID,
        sql="queries/DML_d_product_update.sql",
        dag=dag
    )

    update_d_user_task = SQLExecuteQueryOperator(
        task_id="update_d_user",
        conn_id=ANALYTIC_POSTGRES_CONN_ID,
        sql="queries/DML_d_user_update.sql",
        dag=dag
    )
    
    update_f_user_analytics_task = SQLExecuteQueryOperator(
        task_id="update_f_user_analytics",
        conn_id=ANALYTIC_POSTGRES_CONN_ID,
        sql="queries/DML_f_user_analytics.sql",
        dag=dag
    )

    update_f_sales_task = SQLExecuteQueryOperator(
        task_id="update_f_sales",
        conn_id=ANALYTIC_POSTGRES_CONN_ID,
        sql="queries/DML_f_sales_update.sql",
        dag=dag
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=check_maintenance,
        dag=dag
    )

    notify_api_start_task = HttpOperator(
        task_id="notify_api_start",
        http_conn_id=FASTAPI_CONN_ID,
        endpoint="/maintenance/status",
        method="POST",
        data=json.dumps(
            {"action": "start"}
        ),
        headers={
            "Content-Type": "application/json"
        },
        dag=dag
    )

    run_vacuum_task = SQLExecuteQueryOperator(
        task_id="run_vacuum",
        conn_id=ANALYTIC_POSTGRES_CONN_ID,
        sql="queries/DML_vacuum.sql",
        autocommit=True,
        dag=dag
    )

    notify_api_stop_task = HttpOperator(
        task_id="notify_api_stop",
        http_conn_id=FASTAPI_CONN_ID,
        endpoint="/maintenance/status",
        method="POST",
        data=json.dumps(
            {"action": "stop"}
        ),
        headers={
            "Content-Type": "application/json"
        },
        dag=dag
    )

    skip_maintenance_task = EmptyOperator(
        task_id='skip_maintenance',
        dag=dag
    )


    (
        start_etl_task
        >> extract_data_task
        >> encriment_countries_task
        >> load_data_to_staging_task
        >> ETL_to_migrations_task
        >> update_d_calendar_task
        >> update_d_country_task
        >> update_d_city_task
        >> update_d_product_task
        >> update_d_user_task
        >> update_f_user_analytics_task
        >> update_f_sales_task
        >> branch_task
    )
    (
        branch_task 
        >> [notify_api_start_task, skip_maintenance_task]
    )
    (
        notify_api_start_task
        >> run_vacuum_task 
        >> notify_api_stop_task
    )
