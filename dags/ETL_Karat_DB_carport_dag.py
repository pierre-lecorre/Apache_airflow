import os
import pyodbc
import pandas as pd
import logging
from sqlalchemy import create_engine, MetaData
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=20),
}

# Create the DAG
dag = DAG(
    'carport_availability_db_dag_v1',
    default_args=default_args,
    description='DAG for fetching data and uploading it to Google Sheets',
    schedule_interval='33 16 * * *',
)

VARIABLE_SUFFIX = "carport_availability_db"

# Define the queries and corresponding sheet names
queries = [
    {"query_variable": f"{VARIABLE_SUFFIX}_query_1", "table_name": "AAAA"}
]

# Utility functions
def connect_to_database(db_credentials):
    conn_str = (f"DRIVER={db_credentials['driver']};SERVER={db_credentials['server']};"
                f"DATABASE={db_credentials['database']};UID={db_credentials['username']};"
                f"PWD={db_credentials['password']};TrustServerCertificate=yes;Encrypt=yes;")
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        raise AirflowFailException(f"Database connection failed: {e}")

def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]

def save_to_database(df, db_credentials, table_name):
    try:
        if df.empty:
            logging.warning("DataFrame is empty. Skipping saving to the database.")
            return

        conn_str = f"mssql+pyodbc://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['server']}/{db_credentials['database']}?driver={db_credentials['driver']}"
        engine = create_engine(conn_str)
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Check if the table exists, drop if it does
        if table_name in metadata.tables:
            metadata.tables[table_name].drop(engine)

        # Save DataFrame to SQL Server
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info("DataFrame successfully saved to database.")

    except Exception as e:
        logging.warning(f"Error saving DataFrame to database: {e}")

# Task 1: Load database credentials
def load_credentials(conn_id):
    # Load database credentials from Airflow connection and variables
    try:
        conn = BaseHook.get_connection(conn_id)
        db_credentials = {
            "driver": Variable.get("driver"),
            "server": conn.host,
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password
        }
        return db_credentials
    except Exception as e:
        raise AirflowFailException(f"Error retrieving Airflow variables: {e}")

# Task 2: Run queries and process data
def run_queries_and_process_data(**context):
    db_credentials = load_credentials("karat")
    conn = connect_to_database(db_credentials)

    for item in queries:
        query_variable = item["query_variable"]
        table_name = item["table_name"]

        try:
            query = Variable.get(query_variable)
            data = execute_query(conn, query)
            if data:
                context['ti'].xcom_push(key=f'data_{query_variable}', value={"data": data, "table_name": table_name})
        except KeyError:
            raise AirflowFailException(f"Query variable {query_variable} not found.")
        except Exception as e:
            raise AirflowFailException(f"Error executing query: {e}")
    
    conn.close()

# Task 3: Save results to the database
def save_results_to_bi_db(**context):
    db_credentials = load_credentials("bi_database_conn")  # Replace with actual BI database connection ID

    for item in queries:
        query_variable = item["query_variable"]
        result = context['ti'].xcom_pull(key=f'data_{query_variable}')
        if result:
            df = pd.DataFrame(result.get("data"))
            save_to_database(df, db_credentials, item["table_name"])

# Define the tasks in the DAG
process_queries = PythonOperator(
    task_id='process_queries',
    python_callable=run_queries_and_process_data,
    dag=dag,
)

save_to_db_task = PythonOperator(
    task_id='save_results_to_bi_db',
    python_callable=save_results_to_bi_db,
    dag=dag,
)

# Task dependencies
process_queries >> save_to_db_task
