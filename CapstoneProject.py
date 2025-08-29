# Libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import matplotlib.pyplot as plt
import io
from pathlib import Path

# PostgreSQL connection ID configured in Airflow
PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# --- SQL Query as a variable ---
extracting_data = """
    with sales_summary as (
        select o.orderid, o.orderdate, p.productid, od.quantity, p.price, p.productname
        from
            orders o
        join
            order_details od on o.orderid = od.orderid
        join
            products p on od.productid = p.productid
    )
    select orderdate, round(sum(quantity*price)::numeric, 2) as total_revenue
    from sales_summary
    group by orderdate
    order by 1
"""

# --- Define the DAG ---
with DAG(
    dag_id="Capstone_Sales_Analysis",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["data_pipeline", "sales"],
) as dag:
    
    # --- Task 1: Extract and aggregate data ---
    def extract_and_transform_task(**kwargs):
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        sql = extracting_data
        records = hook.get_records(sql)
        kwargs['ti'].xcom_push(key='daily_revenue_data', value=records)
        print("Data Extracted and Transformed. Result pushed to Xcom.")

    # --- Task 2: Visualize the data ---
    def visualize_revenue_task(**kwargs):
        ti = kwargs['ti']
        daily_revenue_data = ti.xcom_pull(key='daily_revenue_data', task_ids='extract_and_transform_task')
        
        if not daily_revenue_data:
            raise ValueError("No data found in XCom for visualization.")

        df = pd.DataFrame(daily_revenue_data, columns=["order_date", "total_revenue"])
        df["order_date"] = pd.to_datetime(df["order_date"])
        df["total_revenue"] = pd.to_numeric(df["total_revenue"])

        plt.style.use("seaborn-v0_8-whitegrid")
        plt.figure(figsize=(12, 6))
        plt.plot(df["order_date"], df["total_revenue"], marker="o", linestyle="-", color="skyblue")
        plt.title("Daily Sales Revenue Time Series", fontsize=16)
        plt.xlabel("Date", fontsize=12)
        plt.ylabel("Total Revenue ($)", fontsize=12)
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()

        output_dir = Path("/home/kiwilytics/airflow_output/plots")
        output_dir.mkdir(parents=True, exist_ok=True)
        plot_path = output_dir / "daily_revenue_plot.png"
        plt.savefig(plot_path)
        print(f"Daily revenue plot saved to: {plot_path}")

    # --- Task 3: Generate a CSV file ---
    def generate_csv_task(**kwargs):
        ti = kwargs['ti']
        daily_revenue_data = ti.xcom_pull(key='daily_revenue_data', task_ids='extract_and_transform_task')

        if not daily_revenue_data:
            raise ValueError("No data found in XCom to generate CSV.")
        
        df = pd.DataFrame(daily_revenue_data, columns=["order_date", "total_revenue"])
        
        # Define the output path where the CSV will be saved
        output_dir = Path("/home/kiwilytics/airflow_output/data")
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / "daily_revenue_Capstone.csv"
        
        df.to_csv(csv_path, index=False)
        print(f"CSV file generated at: {csv_path}")

    # --- Define the tasks ---
    extract_and_transform = PythonOperator(
        task_id='extract_and_transform_task',
        python_callable=extract_and_transform_task
    )

    visualize_revenue = PythonOperator(
        task_id='visualize_revenue_task',
        python_callable=visualize_revenue_task
    )

    generate_csv = PythonOperator(
        task_id='generate_csv_task',
        python_callable=generate_csv_task
    )
    
    # --- Set the dependencies ---
    extract_and_transform >> [visualize_revenue, generate_csv]
