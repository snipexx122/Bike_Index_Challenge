import json
import requests
import datetime as dt 
# import data ingester object
from Data_Ingestion_script.Data_Ingestion import data_ingester

#./spark-submit --master spark://spark-master:7077  /opt/spark-apps/simple_example.py

# [START instantiate_dag]
# @dag(
#     schedule_interval="0 0 * * *",                      #interval how often the dag will run (can be cron expression as string)
#     start_date=pendulum.datetime(2023, 2, 4, tz="UTC"), # from what point on the dag will run (will only be scheduled after this date)
#     catchup=False,                                      # no catchup needed, because we are running an api that returns now values                
#     tags=['task'],                                      # tag the DAQ so it's easy to find in AirflowUI
# )


def extract(engine,year,manufacturer,colors,stoleness):
    counter=1
    data = {'bikes':[]}
    
    while True:
        data_ingested = engine.generate_data(counter,str(year),manufacturer,colors,stoleness)
        if data_ingested.get("bikes") == [] and year==2023:
            break
        
        if data_ingested.get("bikes") == []:
            year+=1
            counter=1
        else:
            counter+=1
            
        
        data['bikes']+=data_ingested['bikes']
        print(counter)
    return data

def start_extraction():
    engine = data_ingester()
    bike_data = extract(engine,2020,"","","")
        
    return bike_data 







