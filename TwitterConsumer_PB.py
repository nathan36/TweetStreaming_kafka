import json
from kafka import KafkaConsumer
import requests
from datetime import datetime
import time
import re
import configparser

def group_by_15m(min: int) -> int:
    return min//10*10 if min//10 !=0 else (min//10+1)*10

def create_post_data(label: str, input: dict) -> list:
    return [{'id': str(input['id']),
             'body': input['text'],
             'label': label,
             'year': datetime.now().year,
             'month': datetime.now().month,
             'day': datetime.now().day,
             'hour': datetime.now().hour,
             'minute': group_by_15m(datetime.now().minute),
             'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]

def post_message(message, endpoint):
    res = requests.post(endpoint, json.dumps(message).encode('utf-8'))
    print("status:{} | message={}".format(res.status_code, message))

if __name__ == '__main__':
    config = configparser.RawConfigParser()
    config.read('config.ini')
    server_ip = config['KAFKA']['server_ip']
    api_endpoint = config['POWER_BI']['endpoint']
    topic = 'Tweets'
    wide_search = '$'
    narrow_search = '^\$[A-Z]+'

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server_ip,
        auto_offset_reset='latest'
    )
    for message in consumer:
        j = json.loads(message.value)
        data = j['data']
        if wide_search in data['text']:
            for label in data['text'].split(' '):
                if re.search(narrow_search, label):
                    message = create_post_data(label, data)
                    post_message(message, api_endpoint)
        print("message={}".format(data['text']))
        time.sleep(0.5)