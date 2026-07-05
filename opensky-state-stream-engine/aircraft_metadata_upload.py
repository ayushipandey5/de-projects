import requests
import csv
import json
from io import StringIO
from confluent_kafka import Producer

brokers = "localhost:19092,localhost:29092,localhost:39092"
topic = "aircraft_registry"

conf = {
    'bootstrap.servers': brokers,
    'client.id': 'registry-bootstrapper'
}
producer = Producer(conf)

CSV_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"

response = requests.get(CSV_URL)
response.raise_for_status()

csv_data = StringIO(response.text)
reader = csv.DictReader(csv_data)

def delivery_report(err,msg):
    if err is not None:
        print(f"Failed to deliver record: {err}")

count = 0

for row in reader:
    icao24 = row.get('icao24','').strip()

    if not icao24:
        continue
    
    payload = {
        "manufacturer": row.get('manufacturername', 'Unknown').strip(),
        "model": row.get('model', 'Unknown').strip(),
        "airline": row.get('operator', 'Unknown').strip()
    }

    producer.produce(
        topic = topic,
        key = icao24.encode('utf-8'),
        value = json.dumps(payload).encode('utf-8'),
        callback=delivery_report
    )

    count += 1

    if count % 10000 == 0:
        print(f"Published {count} aircraft records...")
        producer.poll(0)
    
producer.flush()
print(f"SUCCESS! Fully seeded the aircraft_registry topic with {count} total records.")



