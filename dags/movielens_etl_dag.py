from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, chain
from datetime import timedelta
import sys
import os
import logging
import pendulum

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from scripts.download import download_file, unzip_file
from scripts.config import MOVIELENS_URL, ZIP_FILE_PATH, RAW_DATA_DIR
from scripts.database import get_engine
from scripts.stage_load import load_movies_to_staging, load_ratings_to_staging
from scripts.validate import run_validations
from scripts.transform import transform_data
from scripts.analysis import run_analytics

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='movielens_etl_pipeline',
    default_args=default_args,
    description='A data pipeline to process MovieLens data',
    schedule='0 12 * * *', # Daily at 12:00 PM
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['movielens', 'etl'],
)
def movielens_etl():
    start_task = EmptyOperator(task_id='start_pipeline')

    @task
    def download_task_func():
        # Wrapper to call download functions
        logging.info("Starting Download Task")
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        download_file(MOVIELENS_URL, ZIP_FILE_PATH)
        unzip_file(ZIP_FILE_PATH, RAW_DATA_DIR)

    @task
    def stage_load_task_func():
        engine = get_engine()
        load_movies_to_staging(engine)
        load_ratings_to_staging(engine)

    @task
    def validate_task_func():
        engine = get_engine()
        run_validations(engine)

    @task
    def transform_task_func():
        engine = get_engine()
        transform_data(engine)

    @task
    def analysis_task_func():
        engine = get_engine()
        run_analytics(engine)

    t1 = download_task_func()
    t2 = stage_load_task_func()
    t3 = validate_task_func()
    t4 = transform_task_func()
    t5 = analysis_task_func()

    # Define dependencies
    chain(start_task, t1,t2,t3,t4,t5)

etl_dag = movielens_etl()
