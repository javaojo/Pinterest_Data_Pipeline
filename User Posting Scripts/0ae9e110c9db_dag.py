from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
from airflow import DAG
import creds

# Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/[INSERT_DATABRICKS_EMAIL_HERE]/Batch_Processing_S3_Data'
}

# Define params for Run Now Operator
notebook_params = {
    "Variable": 5
}

default_args = {
    'owner': '0ae9e110c9db_dag',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('0ae9e110c9db_dag',
         # should be a datetime format
         start_date=datetime(2024, 1, 3),
         # check out possible intervals, should be a string
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args
         ) as dag:
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id=creds.databricks_conn_id,
        existing_cluster_id=creds.existing_cluster_id,
        notebook_task=notebook_task
    )
    opr_submit_run
