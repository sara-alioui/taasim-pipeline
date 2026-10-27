import time
import json
import random
from confluent_kafka import Producer
import pandas as pd

conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)
topic_name = 'raw.gps'

df = pd.read_csv('/home/jovyan/datasets/train.csv', nrows=1000)
valid_rows = [(str(row['TAXI_ID']), row['POLYLINE']) for _, row in df.iterrows() if row['POLYLINE'] != '[]']

print(f'Producer continu demarre - {len(valid_rows)} trajets')
print('Ctrl+C pour arreter')

try:
    cycle = 0
    while True:
        cycle += 1
        sample = random.sample(valid_rows, min(50, len(valid_rows)))
        for taxi_id, polyline in sample:
            points = polyline.strip('[]').split('],[')
            first_point_str = points[0].replace('[', '').replace(']', '')
            lon, lat = map(float, first_point_str.split(','))
            casa_lon = -7.70 + ((lon - (-8.73)) / 0.23 * 0.15)
            casa_lat = 33.50 + ((lat - 41.10) / 0.15 * 0.10)
            message = {
                'taxi_id': taxi_id,
                'timestamp': int(time.time() * 1000),
                'lat': casa_lat,
                'lon': casa_lon,
                'status': random.choice(['available', 'busy', 'available', 'available'])
            }
            producer.produce(topic_name, key=taxi_id, value=json.dumps(message))
            producer.poll(0)
        producer.flush()
        print(f'Cycle {cycle}: 50 messages envoyes a {time.strftime("%H:%M:%S")}')
        time.sleep(3)
except KeyboardInterrupt:
    print('Arret du producer')