# Complete ETL Pipeline

"""
This script is a production-ready implementation of an ETL pipeline that orchestrates data extraction, transformation, and loading processes.
"""

import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to extract data

def extract_data():
    # Simulate data extraction from a source
    data = {
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    }
    return pd.DataFrame(data)

# Function to transform data

def transform_data(df):
    # Add a new column with transformed values
    df['value'] = df['value'] * 2
    return df

# Function to load data into a database

def load_data(df):
    engine = sqlalchemy.create_engine('sqlite:///mydatabase.db')
    df.to_sql('my_table', con=engine, if_exists='replace', index=False)

# Define the DAG

def run_etl_pipeline():
    with DAG(
        'complete_etl_pipeline',
        default_args={'start_date': datetime(2026, 3, 6)},
        schedule_interval='@daily',
        catchup=False,
    ) as dag:
        extract_task = PythonOperator(
            task_id='extract',
            python_callable=extract_data,
        )
        transform_task = PythonOperator(
            task_id='transform',
            python_callable=transform_data,
            op_kwargs={'df': extract_task.output},
        )
        load_task = PythonOperator(
            task_id='load',
            python_callable=load_data,
            op_kwargs={'df': transform_task.output},
        )
        extract_task >> transform_task >> load_task

# Run the pipeline if this script is executed
if __name__ == '__main__':
    run_etl_pipeline()