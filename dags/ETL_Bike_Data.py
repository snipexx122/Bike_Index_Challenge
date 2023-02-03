
# imports important for Airflow
import pendulum
from airflow.decorators import dag, task

# Import Modules for code
import json
import requests
import pandas as pd
from pandas import DataFrame, json_normalize
import datetime as dt
import psycopg2 

# import data ingester object
from Data_Ingestion_script.Data_Ingestion import data_ingester


# [START instantiate_dag]
@dag(
    schedule_interval="0 0 * * *",                      #interval how often the dag will run (can be cron expression as string)
    start_date=pendulum.datetime(2023, 2, 4, tz="UTC"), # from what point on the dag will run (will only be scheduled after this date)
    catchup=False,                                      # no catchup needed, because we are running an api that returns now values                
    tags=['task'],                                      # tag the DAQ so it's easy to find in AirflowUI
)
def ETL_Bike_Data():
    @task()
    def extract(engine,year,manufacturer,colors,stoleness):

       data = engine.generate_data(year,manufacturer,colors,stoleness)

       print(data)

       return data 


    # TRANSFORM: Transform the API response into something that is useful for the load
    @task()
    def transform(bike_json):
        
        return      bike_json

    # LOAD: Save the data into Postgres database
    @task()
    def load(bike_data):
        
        try:
            connection = psycopg2.connect(user="airflow",
                                        password="airflow",
                                        host="postgres",
                                        port="5432",
                                        database="WeatherData")
            cursor = connection.cursor()

            # postgres_insert_query = """INSERT INTO temperature (location, temp_c, wind_kph, time) VALUES ( %s , %s, %s, %s);"""
            # record_to_insert = (weather_data[0]["location"], weather_data[0]["temp_c"], weather_data[0]["wind_kph"], weather_data[0]["timestamp"])
            # cursor.execute(postgres_insert_query, record_to_insert)

            # connection.commit()
            #count = cursor.rowcount
            #print(count, "Record inserted successfully into table")

        except (Exception, psycopg2.Error) as error:
            
            print("Failed to insert record into mobile table", error)
            
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
            
            raise Exception(error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")


    
    # Define the main flow
    engine = data_ingester()
    bike_data = extract(engine,"2023","","","")
    #bike_summary = transform(bike_data)
    #load(bike_summary)


# Invocate the DAG
load_bike_data = ETL_Bike_Data()

