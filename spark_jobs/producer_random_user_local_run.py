import requests
import json
from time import sleep
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',  # ← this is the correct value inside the container
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    res = requests.get('https://randomuser.me/api/')
    if res.status_code == 200:
        user_data = res.json()
        producer.send('random_users', user_data)
        print("Sent:", user_data['results'][0]['login']['username'])
    else:
        print("Failed:", res.status_code)
    sleep(0.5)

    #docker exec -it spark_validation_platform bash
    #python /opt/spark/jobs/producer_random_user.py # run in container