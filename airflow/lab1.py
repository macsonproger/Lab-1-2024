from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import pandas as pd
from elasticsearch import Elasticsearch
import os

def load_csv_files(input_dir):
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    dataframes = []
    for file in csv_files:
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path, dtype={'designation': 'str', 'region_1': 'str'})  
        dataframes.append(df)
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.to_csv('Combined.csv', index=False) 
    
def clean_data():
    combined_df = pd.read_csv('Combined.csv')
    combined_df = combined_df.dropna(subset=['designation', 'region_1'])
    combined_df.to_csv('Combined.csv', index=False)
    
def fill_missing_prices():
    combined_df = pd.read_csv('Combined.csv')
    combined_df = combined_df.fillna({'price': 0.0})
    combined_df.to_csv('Combined.csv', index=False)
    
    
def save_processed_data(output_dir):
    combined_df = pd.read_csv('Combined.csv')
    output_file_path = os.path.join(output_dir, 'processed.csv')
    combined_df.to_csv(output_file_path, index=False)
    
def upload_to_elasticsearch():
    combined_df = pd.read_csv('Combined.csv')
    es = Elasticsearch("http://elasticsearch-kibana:9200")
    for _, row in combined_df.iterrows():
        es.index(index='wine_new', body=row.to_json())
        
        
input_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'input')  
output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'output') 


with DAG(
    dag_id="Lab_1_Pipeline",
    schedule="0 0 * * *", 
    start_date=datetime(2024, 11, 30, tzinfo=timezone.utc), 
    catchup=False,  
    dagrun_timeout=timedelta(minutes=60),  
    tags=["Lab"],
) as dag:

    task_load = PythonOperator(
        task_id='load_csv_files',  
        python_callable=load_csv_files,  
        op_args=[input_directory],
        provide_context=True  
    )

    task_clean = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
    )

    task_fill = PythonOperator(
        task_id='fill_missing_prices',
        python_callable=fill_missing_prices,
        provide_context=True
    )

    task_save = PythonOperator(
        task_id='save_processed_data',
        python_callable=save_processed_data,  
        op_args=[output_directory],
        provide_context=True  
    )
    task_upload = PythonOperator(
        task_id='upload_to_elasticsearch',
        python_callable=upload_to_elasticsearch,
        provide_context=True  
    )

    task_load >> task_clean >> task_fill >> [task_save, task_upload]