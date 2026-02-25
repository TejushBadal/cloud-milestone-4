import json
import os
from google.cloud import pubsub_v1

# Setup
PROJECT_ID = "REDACTED"
TOPIC_NAME = "smart-meter"
SUB_NAME = "filter-sub"

# Use the key you already made
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "voting_machine/key.json"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUB_NAME)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    
    # If it already has a status, it's been processed. Ignore to avoid loops.
    if "status" in data:
        message.ack()
        return

    # Eliminate records with missing measurements (None)
    if data.get("pressure") is not None and data.get("temperature") is not None:
        print(f"Filter: Data is valid. Tagging as filtered.")
        data["status"] = "filtered"
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    else:
        print("Filter: Dropped record containing None values.")
    
    message.ack()

print(f"FilterReading service started on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()