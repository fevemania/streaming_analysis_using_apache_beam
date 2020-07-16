from flask import request
from flask_restful import Resource
from http import HTTPStatus
import os
from flask import current_app
from google.cloud import pubsub_v1
import json
import hashlib
import pdb
from api_server.extensions import get_millis_since_epoch
from datetime import datetime
import time

def get_callback(f, fs, logical_id):
    def callback(f):
        try:
            f.result()
            fs.pop(logical_id)
        except:  # noqa
            print("Please handle {} for id {}.".format(f.exception(), 
                logical_id))
            time.sleep(5)

    return callback

class StreamResource(Resource):
    def post(self):
        json_data = request.get_json()
        json_payload = json.dumps(json_data).encode('utf-8')
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            current_app.config['PROJECT_ID'],
            current_app.config['PUBSUB_TOPIC'])
        
       #fs = []
        fs = dict()
        for msg in json_data:
            #json_payload = json.dumps(msg).encode('utf-8')
            dropoff_datetime = datetime.strptime(json.loads(msg)['dropoff_datetime'], "%Y-%m-%d %H:%M:%S")
            event_time_str = str(get_millis_since_epoch(dropoff_datetime))
            json_payload = msg.encode('utf-8')
            md5 = hashlib.md5()
            md5.update(json_payload)
            logical_id = md5.hexdigest()

            fs.update({logical_id: None})
            future = publisher.publish(topic_path,
                                   json_payload,
                                   logical_id=logical_id,
                                   event_time=event_time_str)
            fs[logical_id] = future
            future.add_done_callback(get_callback(future, fs, logical_id))
        
       #md5 = hashlib.md5()
       #md5.update(json_payload)
       #logical_id = md5.hexdigest() 
       #fs.update({logical_id: None})
       #future = publisher.publish(topic_path,
       #                           json_payload,
       #                           logical_id=logical_id)
       #fs[logical_id] = future
       #
       #future.add_done_callback(get_callback(future, fs, logical_id))
        while fs:
            time.sleep(5)
       #for f in fs:
       #    f.result()
        return None, HTTPStatus.CREATED
