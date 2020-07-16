from google.cloud import bigquery
import requests
import threading

import psycopg2
import simplejson as json
#import json
import decimal
from datetime import datetime
import math
import pdb
import time

data = []
url = 'http://localhost:5000/streams'
#url = 'https://csvupload-tamk4seawq-de.a.run.app/streams' 
num_data = 1000

def job(num, start):
    partial_data = data[start:start+num_data]
    if len(partial_data) > 0:
        response = requests.post(url, json=partial_data)
        while response.status_code == 500:
            time.sleep(10)
            print('pub/sub failed')
            response = requests.post(url, json=paritial_data)

def func():
    client = bigquery.Client()

    query = """
        SELECT TO_JSON_STRING(t)
        FROM `prac.new_york_yellow_taxi_trips_20151019` t
    """

    query_job = client.query(query)
    #results = query_job.result()
    print('good')
    for row in query_job:
        msg = json.loads(row.f0_, use_decimal=True)
        msg['pickup_datetime'] = datetime.strptime(msg['pickup_datetime'], 
            "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        msg['dropoff_datetime'] = datetime.strptime(msg['dropoff_datetime'],
            "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        msg = json.dumps(msg, use_decimal=True)
        data.append(msg)
    print('good2')
    threads = []
    num_threads = math.ceil(len(data)/num_data)
   #num_threads = 1
    for i in range(num_threads):
        threads.append(threading.Thread(target = job, args = (i, i*num_data)))
        threads[i].start()
        if i % 50 == 0:
            time.sleep(5)
    for i in range(num_threads):
        threads[i].join()

func()
