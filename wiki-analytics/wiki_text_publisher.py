import json
import sseclient
import requests
import os
from google.cloud import pubsub_v1


project_id = "wiki-analytics-1"
topic_id = "wiki-global"
url = "https://stream.wikimedia.org/v2/stream/recentchange"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id,topic_id)
try:
    topic = publisher.get_topic(request={"name": topic_path})
    print(f"Topic {topic_path} found.")
except Exception as e:
    print(f"Topic {topic_path} not found. Creating it now...")
    topic = publisher.create_topic(request={"name": topic_path})

headers = {
    'User-Agent': 'WikiEditsAnalyticsPortfolioProject/1.0'
}

response = requests.get(url,stream=True,headers=headers)
if response.status_code == 200:
    client = sseclient.SSEClient(response)

    for event in client.events():
        if event.event == "message":
            try:
                data = json.loads(event.data)
                if data.get("type") == "edit":
                    is_bot_str = str(data.get("bot",False)).lower()

                    publish_future = publisher.publish(
                        topic_path,
                        data = event.data.encode('utf-8'),
                        is_bot=is_bot_str,
                        event_type="edit"
                    )
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error: {e}")

else:
    print(f"Failed to connect. Server returned: {response.text}")
