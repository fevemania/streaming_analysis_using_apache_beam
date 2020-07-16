import sqlalchemy
from database import engine
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import time
import json
import pdb
import os
import threading
from database import Test201510191900
import decimal

# TODO(developer)
# project_id = "your-project-id"
# subscription_id = "your-subscription-id"
# Number of seconds the subscriber should listen for messages
# timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
PROJECT_ID = "project_name"
PUBSUB_SUBSCRIPTION = "dedup_v1"
subscription_path = subscriber.subscription_path(PROJECT_ID, PUBSUB_SUBSCRIPTION)

count = 0

def callback(message):
    item_list = json.loads(message.data.decode('utf-8'))
    json_data = []
    for item in item_list:
        json_data.append(json.loads(item, parse_float=decimal.Decimal))
    with engine.begin() as conn:
        conn.execute(
            Test201510191900.__table__.insert(),
            json_data
        )
   #msg = json.loads(message.data.decode('utf-8'))
   #print(message, msg)
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for messages on {}..\n".format(subscription_path))

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
