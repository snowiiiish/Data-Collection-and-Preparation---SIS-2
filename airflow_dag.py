"""
Airflow DAG for F1 Drivers Data Pipeline
Scrapes, cleans, and loads F1 data into SQLite database
Runs no more than once per day
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

project_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_dir, 'src')
data_dir = os.path.join(project_dir, 'data')

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)


raw_file = os.path.join(data_dir, "drivers.json")
clean_file = os.path.join(data_dir, "drivers_clean.json")
db_file = os.path.join(data_dir, "output.db")

from scraper import scrape_f1_drivers
from cleaner import load_raw_data, clean_and_aggregate_data, save_cleaned_data
from loader import load_cleaned_data, insert_data_to_db, verify_database

default_args = {
    'owner': 'sis2_dcp_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'f1_drivers_pipeline',
    default_args=default_args,
    description='F1 scraping, cleaning, and loading pipeline',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['f1', 'scraping', 'etl'],
)



def scrape_task(**context):
    logging.info("Starting F1 scraping task")
    
    try:
        scrape_f1_drivers()
        logging.info("Scraping completed successfully.")
    except Exception as e:
        logging.error(f"Scraping task failed: {e}")
        raise


def clean_task(**context):
    logging.info("Starting data cleaning task")
    
    try:
        raw_data = load_raw_data(raw_file)
        
        if not raw_data:
            logging.warning("No raw data found to clean.")
            return

        cleaned_data = clean_and_aggregate_data(raw_data)
        
        save_cleaned_data(cleaned_data, clean_file)
        
        logging.info(f"Cleaning completed. {len(cleaned_data)} drivers processed.")
    except Exception as e:
        logging.error(f"Cleaning task failed: {e}")
        raise


def load_task(**context):
    logging.info("Starting data loading task")
    
    try:
        cleaned_data = load_cleaned_data(clean_file)
        
        if not cleaned_data:
            logging.warning("No cleaned data found to load.")
            return

        insert_data_to_db(cleaned_data, db_file)
        
        logging.info(f"Loading completed.")
    except Exception as e:
        logging.error(f"Loading task failed: {e}")
        raise


scrape = PythonOperator(
    task_id='scrape_f1_drivers',
    python_callable=scrape_task,
    dag=dag,
)

clean = PythonOperator(
    task_id='clean_data',
    python_callable=clean_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load_to_database',
    python_callable=load_task,
    dag=dag,
)

scrape >> clean >> load

if __name__ == "__main__":
    dag.test()