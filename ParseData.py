import json
import os
import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from Variables import *

# Download JSON from API's response
def getData():
    try:
        response = requests.get(f"{url}?api-key={api_key}&format={format}&offset={offset}&limit={limit}")
        date_time = datetime.now()
        date = date_time.strftime("%d-%m-%y")
        with open(f'jsondata/{date}.json', 'w+') as file:
            json.dump(response.json(), file)

    except requests.exceptions.HTTPError as http_err:
        return f'HTTP error occurred: {http_err}'

    except Exception as err:
        return f'Other error occurred: {err}'

# create database schema in RDS.
def create_schema():
  try:
    sqlcon = mysql.connector.connect(
        host=database_info['host'],
        user=database_info['user'],
        password=database_info['password']
    )
    cursor = sqlcon.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {database_info['databaseName']}")
    cursor.execute(f"CREATE DATABASE {database_info['databaseName']}")
    print(f"db '{database_info['databaseName']}' created successfully")
    cursor.execute(f"use {database_info['databaseName']}")
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {database_info['table']}
                    (id INT,
                    country VARCHAR(100),
                    state VARCHAR(100),
                    city VARCHAR(100),
                    station VARCHAR(255),
                    last_update DATE,
                    pollutant_id VARCHAR(10),
                    pollutant_min INT,
                    pollutant_max INT,
                    pollutant_avg INT,
                    PRIMARY KEY (id, last_update))
                  """)
    sqlcon.commit()
    print(f"Table '{database_info['table']}' created successfully")
  except Error as e:
   print(e)

# Insert Query
def insertData(response):
    sqlcon = mysql.connector.connect(
        host=database_info['host'],
        user=database_info['user'],
        password=database_info['password']
    )
    cursor = sqlcon.cursor()
    # To count the records inserted.
    i=0
    for record in response['records']:
      i+=1
      try:
        query = f"""INSERT INTO {database_info['databaseName']}.{database_info['table']} (id, country, state, city, station, last_update, pollutant_id, pollutant_min, pollutant_max, pollutant_avg) VALUES ({record['id']}, "{record['country']}", "{record['state']}", "{record['city']}", "{record['station']}", '{datetime.strptime(record['last_update'], "%d-%m-%Y %H:%M:%S")}', "{record['pollutant_id']}", {record.get('pollutant_min') or 'NULL'}, {record.get('pollutant_max') or 'NULL'}, {record.get('pollutant_avg') or 'NULL'})"""

        cursor.execute(query)
        sqlcon.commit()
        print(f"{i} records inserted.")
      except Error as e:
        print(f"error is: {e} at {i}")

# Function contains fetch, fetch, create schema, insert data in RDS
def fetchData():
    json_path = "/home/bhupesh/Desktop/air_quality_index/jsondata/"
    create_schema()
    for file in os.listdir(json_path):
        with open(os.path.join(json_path, file), 'r') as json_file:
            data = json.load(json_file)
        insertData(data)
    
if __name__ == '__main__':
   fetchData()