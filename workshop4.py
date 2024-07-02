from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests


MYSQL_CONNECTION = "mysql_default"   # connection name
CONVERSION_RATE_URL = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"

# output path
mysql_output_path = "/home/airflow/gcs/data/transaction_data_merged.parquet"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.parquet"
final_output_path = "/home/airflow/gcs/data/workshop4_output.parquet"

default_args = {
    'owner': 'nannaphasb',
}

@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), tags=["workshop"])
def workshop4_pipeline():
    
    @task()
    def get_data_from_mysql(output_path):
        # Receive output_path from the task

        # Use MySqlHook to connect to MySQL from a connection created in Airflow
        mysqlserver = MySqlHook(MYSQL_CONNECTION)
        
        # Query the database using the created Hook to get the results as a pandas DataFrame
        product = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.product")
        customer = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.customer")
        transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.transaction")

        # Merge data from 2 DataFrames as in workshop1
        merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")
        
        # Save the parquet file to the input output_path, which will be automatically stored in GCS
        merged_transaction.to_parquet(output_path, index=False)
        print(f"Output to {output_path}")

    @task()
    def get_conversion_rate(output_path):
        # Send a request to get data from CONVERSION_RATE_URL
        r = requests.get(CONVERSION_RATE_URL)
        result_conversion_rate = r.json()
        df = pd.DataFrame(result_conversion_rate)
        df = df.drop(columns=['id'])

        # Convert the column to date format and save the file as a parquet file
        df['date'] = pd.to_datetime(df['date'])
        df.to_parquet(output_path, index=False)
        print(f"Output to {output_path}")

    @task()
    def merge_data(transaction_path, conversion_rate_path, output_path):

        # Read data
        transaction = pd.read_parquet(transaction_path)
        conversion_rate = pd.read_parquet(conversion_rate_path)

        # merge 2 DataFrame
        final_df = transaction.merge(conversion_rate, how="left", left_on="Date", right_on="date")
        
        # Transform data
        final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
        final_df["thb_amount"] = final_df["total_amount"] * final_df["gbp_thb"]

        # drop the column and rename the columns
        final_df = final_df.drop(["date", "gbp_thb"], axis=1)

        final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
            'product_name', 'customer_country', 'customer_name', 'total_amount','thb_amount']

        # save the parquet file
        final_df.to_parquet(output_path, index=False)
        print(f"Output to {output_path}")

    
    t1 = get_data_from_mysql(output_path=mysql_output_path)

    t2 = get_conversion_rate(output_path=conversion_rate_output_path)

    t3 = merge_data(
            transaction_path=mysql_output_path,
            conversion_rate_path=conversion_rate_output_path,
            output_path=final_output_path
        )

    [t1, t2] >> t3

workshop4_pipeline()