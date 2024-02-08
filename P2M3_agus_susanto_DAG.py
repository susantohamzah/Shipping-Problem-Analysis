'''
=================================================
Milestone 3

Name  : Agus Susanto
Batch : FTDS-026-RMT

The program is created to automate the transformation and loading of data from PostgreSQL to Elasticsearch. The dataset used is related to the supply chain in a sales company.
=================================================
'''

import psycopg2
import re
import pandas as pd
import datetime as dt

#Library Menghitung durasi waktu
from datetime import timedelta
import warnings
from elasticsearch import Elasticsearch
warnings.filterwarnings("ignore")

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def fetch_data():
    '''
    Purpose:
    This function retrieves data from a PostgreSQL database using the provided connection details. The retrieved data is then stored in a CSV file in the directory '/opt/airflow/dags/'.

    Input:
    No input is required.

    Output:
    A CSV file containing data from the 'supply_chain' table is saved in '/opt/airflow/dags/' with the name 'P2M3_agus_susanto_data_raw.csv'.
    
    How to Use?:
    Simply call the function fetch_data()
        
    '''
    
    #Defined Database detail
    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432'
    
    #Definded connection
    connection = psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    )
    
    #Query select from database
    select_query = 'SELECT * From supply_chain;'
    
    #Create dataframe
    df = pd.read_sql(select_query, connection)
    
    connection.close()
    
    #Save dataframe into csv
    df.to_csv('/opt/airflow/dags/P2M3_agus_susanto_data_raw.csv', index=False)
    
    
def data_cleaning():
    
    '''
    Purpose:
    This function is used to clean the data retrieved from the database. Data cleaning involves changing column names to lowercase and replacing spaces with underscores. Additionally, the function fills in empty values in the 'customer_lname', 'customer_zipcode', 'order_zipcode', and 'product_description' columns with default values.

    Input:
    Data from the CSV file 'P2M3_agus_susanto_data_raw.csv'.

    Output:
    A CSV file containing cleaned data is saved in '/opt/airflow/dags/' with the name 'P2M3_agus_susanto_data_clean.csv'.
    
    How to Use?:
    Simply call the function data_cleaning()
    '''
    
    #read cvs
    df = pd.read_csv('/opt/airflow/dags/P2M3_agus_susanto_data_raw.csv')
    
    #Modify column name
    df_cleaned = df.columns.str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.lower()
    df.columns = df_cleaned
    
    #handling missing value in customer_lname
    df['customer_lname'].fillna(df['customer_fname'], inplace=True)
    
    #handling missing value in customer_zipcode
    df['customer_zipcode'].fillna('-', inplace=True)

    #handling missing value in order_zipcode
    df['order_zipcode'].fillna('-', inplace=True)

    #handling missing value in product_description
    df['product_description'].fillna('-', inplace=True)
    
    #Save to csv
    df.to_csv('/opt/airflow/dags/P2M3_agus_susanto_data_clean.csv')
    
def insert_into_elastic_manual():
    '''
    Purpose:
    This function is responsible for inserting the cleaned data into an Elasticsearch database. The data is processed row by row and indexed into the 'supply_chain' index.

    Input:
    Data from the CSV file 'P2M3_agus_susanto_data_clean.csv'.

    Output:
    Data is inserted into the Elasticsearch database with the index 'supply_chain'. If there is a failure, information about the failed data insertion is logged.

    How to Use?::
    Simply call the function insert_into_elastic_manual().
    '''
    #read csv
    df = pd.read_csv('/opt/airflow/dags/P2M3_agus_susanto_data_clean.csv')

    #Connection to elastic
    es = Elasticsearch('http://elasticsearch:9200')
    print('Connection Status : ', es.ping())
    
    failed_insert = []
    
    #Looping to input every data from csv to elastic
    for i, r in df.iterrows():
        doc = r.to_json()
        try:
            print(i, r['order_item_id'])
            res = es.index(index='supply_chain', doc_type="doc", body=doc)
        except Exception as e:
            print('Index Gagal : ', r['order_item_id'])
            print(f'Error: {e}')
            failed_insert.append(r['order_item_id'])
            pass
        
    print('DONE')
    print('Failed Insert : ', failed_insert)


# Define the default arguments    
default_args = {
    'owner': 'agus',
    'start_date': dt.datetime(2024, 1, 26, 3, 30, 0)- dt.timedelta(hours=8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Define the DAG    
with DAG('SupplyChainDB',
         default_args=default_args,
         schedule_interval='30 6 * * *',
         catchup=False
         ) as dag:

    #Description for every task
    node_start = BashOperator(
        task_id='starting',
        bash_command='echo "I am reading the CSV Now ......"'
    )
    
    #Running fetch_data() in DAG
    node_fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data
    )
    
    #Running data_cleaning() in DAG
    node_data_cleaning = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning
    )
    
    #Running insert_data_to_elastic() in DAG
    node_insert_data_to_elastic = PythonOperator(
        task_id='insert_data_to_elastic',
        python_callable=insert_into_elastic_manual
    )

# running all process with priority    
node_start >> node_fetch_data >> node_data_cleaning >> node_insert_data_to_elastic