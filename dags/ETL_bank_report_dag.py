import os
import pyodbc
import pandas as pd
import gspread
import gspread_dataframe as gd
import tempfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import json
from decimal import Decimal
from datetime import date  # Ensure date is imported


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
    'bank_report_dag_v1',
    default_args=default_args,
    description='DAG for fetching data and uploading it to Google Sheets',
    schedule_interval='35 16 * * *',
)

VARIABLE_SUFFIX = "bank_report"

# Define the queries and corresponding sheet names (used in both tasks)
queries = [
    {"query_variable": f"{VARIABLE_SUFFIX}_query_1", "sheet_name": "output_query_bank_statements"},

]

# Utility functions
def connect_to_database(driver, server_name, database_name, username, password):
    conn_str = (f"DRIVER={driver};SERVER={server_name};"
                f"DATABASE={database_name};UID={username};"
                f"PWD={password};TrustServerCertificate=yes;"
                f"Encrypt=yes;")
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

def save_to_gs(data, worksheet_name, gsheet_name, gc):
    spreadsheet = gc.open(gsheet_name)
    worksheet_list = spreadsheet.worksheets()
    worksheet_exists = any(worksheet.title == worksheet_name for worksheet in worksheet_list)
    if not worksheet_exists:
        spreadsheet.add_worksheet(title=worksheet_name, rows=1, cols=1)
    worksheet = spreadsheet.worksheet(worksheet_name)
    df = pd.DataFrame(data)
    gd.set_with_dataframe(worksheet, df, include_index=False, include_column_header=True, resize=True)

# Task 1: Connect to the database using Airflow variables
def load_credentials(**context):    
    # Load database credentials from Airflow variables
    try:
        # Retrieve the connection using conn_id="karat"
        conn = BaseHook.get_connection("karat")

        # Access connection details
        driver = Variable.get("driver")
        server_name = conn.host
        database_name = conn.schema
        username = conn.login
        password = conn.password
    except Exception as e:
        raise AirflowFailException(f"Error retrieving Airflow variables: {e}")

    return (driver, server_name, database_name, username, password)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if precision as a string is preferred
        elif isinstance(obj, date):
            return obj.isoformat()  # Converts date to "YYYY-MM-DD"
        return super(DecimalEncoder, self).default(obj)


def run_queries_and_process_data(**context):
    (driver, server_name, database_name, username, password) = load_credentials()
    conn = connect_to_database(driver, server_name, database_name, username, password)

    for item in queries:
        query_variable = item["query_variable"]
        sheet_name = item["sheet_name"]

        if query_variable and sheet_name:
            try:
                query = Variable.get(query_variable)
            except KeyError:
                raise AirflowFailException(f"Query variable {query_variable} not found.")

            if query:
                data = execute_query(conn, query)
                if data:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
                        temp_file.write(json.dumps(data, cls=DecimalEncoder).encode('utf-8'))
                        temp_file.flush()
                        context['ti'].xcom_push(key=f'{query_variable}_file_path', value=temp_file.name)

    conn.close()

# Task 3: Save results to Google Sheets
def save_results_to_google_sheets(**context):
    # Load Google Sheets credentials
    try:
        gsheet_credentials_json = Variable.get("gsheet_credentials_1")
    except Exception as e:
        raise AirflowFailException(f"Error retrieving Google Sheets credentials: {e}")

    try:
        with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
            tmp_file.write(gsheet_credentials_json.encode('utf-8'))
            tmp_file.flush()
            gc = gspread.service_account(filename=tmp_file.name)

            gsheet_name = Variable.get(f"{VARIABLE_SUFFIX}_spreadsheet")

            for item in queries:
                query_variable = item["query_variable"]
                sheet_name = item["sheet_name"]

                # Pull file path from XCom
                file_path = context['ti'].xcom_pull(key=f'{query_variable}_file_path')
                if file_path:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    save_to_gs(data, sheet_name, gsheet_name, gc)

    except Exception as e:
        raise AirflowFailException(f"Error saving to Google Sheets: {e}")

def cleanup_temp_files(**context):
    for item in queries:
        query_variable = item["query_variable"]
        file_path = context['ti'].xcom_pull(key=f'{query_variable}_file_path')
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

# Define the tasks in the DAG
load_db_credentials = PythonOperator(
    task_id='load_db_credentials',
    python_callable=load_credentials,
    dag=dag,
    do_xcom_push=False,
)

process_queries = PythonOperator(
    task_id='process_queries',
    python_callable=run_queries_and_process_data,
    provide_context=True,
    dag=dag,
)

save_to_google_sheets = PythonOperator(
    task_id='save_to_google_sheets',
    python_callable=save_results_to_google_sheets,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    provide_context=True,
    dag=dag,
)

# Task dependencies
load_db_credentials >> process_queries >> save_to_google_sheets >> cleanup_task