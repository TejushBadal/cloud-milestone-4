import json
import os
from google.cloud import pubsub_v1

PROJECT_ID = "REDACTED"
TOPIC_NAME = "smart-meter"
SUB_NAME = "convert-sub"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "voting_machine/key.json"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUB_NAME)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    
    # Only process data that was JUST filtered
    if data.get("status") == "filtered":
        # P(psi) = P(kPa)/6.895
        data["pressure"] = round(data["pressure"] / 6.895, 2)
        # T(F) = T(C)*1.8+32
        data["temperature"] = round((data["temperature"] * 1.8) + 32, 2)
        
        data["status"] = "converted"
        print(f"Convert: Math complete. New values: {data['pressure']} psi, {data['temperature']} F")
        
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    
    message.ack()

print(f"ConvertReading service started on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()