#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name   
    url = params.url
    csv_name = 'output.csv'

    #download the csv file
    os.system(f"wget {url} -O {csv_name}")


    # Using f-string (preferred in modern Python)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()


    df = pd.read_csv('/Users/lakshayanand/Documents/Learning Airflow-Docker/2_DOCKER_SQL/ny_taxi_postgres_data/yellow_tripdata_2021-01.csv', nrows=100)

    print(pd.io.sql.get_schema(df, name = table_name, con=engine))

    df_iter = pd.read_csv('/Users/lakshayanand/Documents/Learning Airflow-Docker/2_DOCKER_SQL/ny_taxi_postgres_data/yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name = table_name, con=engine, if_exists = 'replace')

    while True:
       t_start = time()
       df = next(df_iter)
    
       df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
       df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
     
       df.to_sql(name = table_name, con=engine, if_exists = 'append')
    
       t_end = time()
    
       print("Inserted Another Chunk...took % 3f second" % (t_end -t_start))
     

parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

# user
# password
# host
# port
# database name
# tablename
# url of the csv

if __name__ == '__main__':  

    parser.add_argument('--user', help='username for postgres')    
    parser.add_argument('--password', help='Password for postgres')  
    parser.add_argument('--host', help='host for postgres')  
    parser.add_argument('--port', help='port for postgres')  
    parser.add_argument('--db', help='database name for postgres')  
    parser.add_argument('--table_name', help='name of the table where we will write the results to postgres')  
    parser.add_argument('--url', help='url of the csv')  

    args = parser.parse_args()

    main(args)




