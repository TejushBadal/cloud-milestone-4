import json
import os
import time
from google.cloud import pubsub_v1

PROJECT_ID = "REDACTED"
TOPIC_NAME = "smart-meter"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "voting_machine/key.json"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

def send_test_data():
    # Valid reading
    data_good = {"pressure": 101.325, "temperature": 20.0}
    # Invalid reading
    data_bad = {"pressure": None, "temperature": 25.0}

    for record in [data_good, data_bad]:
        payload = json.dumps(record).encode("utf-8")
        publisher.publish(topic_path, payload)
        print(f"Generator: Sent {record}")
        time.sleep(1)

if __name__ == "__main__":
    send_test_data()