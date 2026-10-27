import time
import json
from confluent_kafka import Producer
import pandas as pd

conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)
topic_name = 'raw.gps'

print('Chargement optimise...')
df = pd.read_csv('/home/jovyan/datasets/train.csv', nrows=1000)
print(f'Dataset charge : {len(df)} lignes')

for index, row in df.iterrows():
    try:
        polyline = row['POLYLINE']
        if polyline == '[]':
            continue
        points = polyline.strip('[]').split('],[')
        first_point_str = points[0].replace('[', '').replace(']', '')
        lon, lat = map(float, first_point_str.split(','))
        ratio_lon = (lon - (-8.73)) / 0.23
        casa_lon = -7.70 + (ratio_lon * 0.15)
        ratio_lat = (lat - 41.10) / 0.15
        casa_lat = 33.50 + (ratio_lat * 0.10)
        message = {
            'taxi_id': str(row['TAXI_ID']),
            'timestamp': int(time.time() * 1000),
            'lat': casa_lat,
            'lon': casa_lon,
            'status': 'available'
        }
        producer.produce(topic_name, key=str(row['TAXI_ID']), value=json.dumps(message))
        producer.poll(0)
        if index % 100 == 0:
            print(f'Envoye : {index} messages')
    except Exception as e:
        print(f'Erreur ligne {index}: {e}')

producer.flush()
print('DONE - messages envoyes vers Kafka')