from pendulum import datetime
from airflow.models import DAG
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain

# Define constants for interacting with external systems
S3_FILE_PATH = "s3://lisonyandu-tech-test/oders"
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake"
SNOWFLAKE_ORDERS = "ORDERS"
# SNOWFLAKE_CUSTOMERS = "customers_table"
# SNOWFLAKE_REPORTING = "reporting_table"


def filter_oders(input_table: Table):
    return "SELECT *FROM {{input_table}} WHERE CITY = 'London'"

with DAG(
    dag_id="orders_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_view="graph"
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # Task 1: Load data from S3 into Snowflake
    #s3://lisonyandu-/oders/small_sample_file.csv
    # creates and updates Dataset("astro://snowflake_default@?table=orders_table")
    orders_data = aql.load_file(
        input_file=File(
            path=S3_FILE_PATH + "/sales_data_sample.csv", 
            conn_id=S3_CONN_ID,
            
          
        ),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            name=SNOWFLAKE_ORDERS
        )
       
    )



    # clean up temporary tables 
    
   # Set task dependencies
    chain(
        begin,
        orders_data,
        end
    )