import os
from datetime import datetime
from airflow import DAG
import json
from preprocess.naver_preprocess import preprocessing

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    "start_date":datetime(2024,1,1)
}

NAVER_CLI_ID = os.getenv("NAVER_CLI_ID")
NAVER_CLI_SECRET = os.getenv("NAVER_CLI_SECRET")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")

NAVER_SEARCH_QUERY = Variable.get("naver_search_keyword", default_var="mango")

def _complete():
    print("네이버 검색 DAG 완료")

with DAG(
    dag_id="naver-search-pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    tags=["naver","search","local","api","pipeline"],
    catchup=False
) as dag:
    
    creating_table = PostgresOperator(
        task_id="creating_table",
        postgres_conn_id="postgres",
        sql='''
            CREATE TABLE IF NOT EXISTS naver_search_result(
                title TEXT,
                address TEXT,
                category TEXT,
                description TEXT,
                link TEXT
            );
        ''',
    )
    
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="naver_search_api",
        endpoint="v1/search/local.json",
        headers={
            "X-Naver-Client-Id" : f"{NAVER_CLI_ID}",
            "X-Naver-Client-Secret" : f"{NAVER_CLI_SECRET}",
        },
        request_params={
            "query" : NAVER_SEARCH_QUERY,
            "display" : 3
        },
        response_check=lambda res: res.json().get("total", 0) > 0
    )
    
    crawl_naver = SimpleHttpOperator(
        task_id="crawl_naver",
        http_conn_id="naver_search_api",
        endpoint=f"v1/search/local.json?query={NAVER_SEARCH_QUERY}&display=3",
        headers={
            "X-Naver-Client-Id" : f"{NAVER_CLI_ID}",
            "X-Naver-Client-Secret" : f"{NAVER_CLI_SECRET}",
        },
        method="GET",
        response_filter=lambda res : json.loads(res.text),
        log_response=True
    )
    
    preprocess_result = PythonOperator(
        task_id="preprocess_result",
        python_callable=preprocessing
    )
    
    store_result = BashOperator(
        task_id="store_naver",
        bash_command=f"""
        PGPASSWORD={POSTGRES_PASSWORD} psql -h {POSTGRES_HOST} -U {POSTGRES_USER} -d {POSTGRES_DB} -c "\\COPY naver_search_result(title, address, category, description, link) FROM '/opt/airflow/dags/data/processed_result.csv' DELIMITER ',' CSV HEADER;"
        """
    )
    
    print_complete = PythonOperator(
        task_id="print_complete",
        python_callable=_complete
    )
    
    creating_table >> is_api_available >> crawl_naver >> preprocess_result >> store_result >> print_complete
    
    