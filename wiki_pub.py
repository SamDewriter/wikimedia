# Wiki Publisher

# It would connect to the endpoint of the Wiki to get data
# It publishes the data to the Pub/Sub topic for further processing

import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

from google.cloud import pubsub_v1

client = pubsub_v1.PublisherClient()
project = os.getenv('PROJECT_ID')
topic = os.getenv('topic')


# Simple request approach


# Output the event to terminal, later we can publish it to Pub/Sub

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

def publish_to_pubsub(event):
    """
    Publish an event to a Google Cloud Pub/Sub topic.

    Args:
        event (dict): The event data to publish.
    """
    try:
        # Convert the event dictionary to a JSON string
        event_data = json.dumps(event).encode("utf-8")
        # Publish the event to the specified Pub/Sub topic
        future = client.publish(f"projects/{project}/topics/{topic}", event_data)
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"Failed to publish message: {e}")


def iter_events(session: requests.Session):
    """
    Iterate over events from the Wikimedia recent changes stream.

    Args:
        session (requests.Session): A requests session object.
    """
    backoff = 1
    while True:
        try: 
            with session.get(STREAM_URL, headers=HEADERS, stream=True, timeout=30) as response:
                response.raise_for_status()
                for raw_line in response.iter_lines(decode_unicode=True):
                    if raw_line is None or not raw_line.startswith('data: '):
                        continue
                    payload = raw_line[len('data: '):]
                    try:
                        yield json.loads(payload)                        
                        # print(f"Received event: {payload}")
                    except json.JSONDecodeError:
                        continue
        except (requests.RequestException, ConnectionError) as e:
            print(f"Error occurred: {e}. Retrying in {backoff} seconds...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)  # Exponential backoff with a max of 60 seconds



# Call the function to start streaming events

if __name__ == "__main__":
    from pprint import pprint
    with requests.Session() as session:
        for event in iter_events(session):
            publish_to_pubsub(event)
            # pprint(event)