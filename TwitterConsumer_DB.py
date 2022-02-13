import json
from kafka import KafkaConsumer
import requests
from datetime import datetime
import time
import re
import configparser
from db import create_connection, write_to_db

def create_post_data(hashtags: list, input: dict) -> list:
    content: dict = {'geo': input['geo'],
                    'retweet_cnt': input['retweet_count'],
                    'created_at': input['created_at'],
                    'coordinates': input['coordinates'],
                    'hashtag': None}
    out = []
    for hashtag in hashtags:
        new_content: dict = content.copy()
        new_content.update(hashtag=hashtag)
        out.append(new_content)
    return out

def post_message(message, endpoint):
    res = requests.post(endpoint, json.dumps(message).encode('utf-8'))
    print("status:{} | message={}".format(res.status_code, message))

if __name__ == '__main__':
    config = configparser.RawConfigParser()
    config.read('config.ini')
    server_ip = config['KAFKA']['server_ip']
    api_endpoint = config['POWER_BI']['endpoint']
    user = config['MYSQL']['user']
    pin = config['MYSQL']['pin']
    host = config['MYSQL']['host']
    port = config['MYSQL']['port']
    dbname = config['MYSQL']['dbname']
    table = 'Tweets'
    topic = 'Tweets'

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server_ip,
        auto_offset_reset='latest'
    )
    for message in consumer:
        json_dic = json.loads(message.value)
        data = json_dic['data']
        hashtags = data['entities']['hashtags']
        conn = create_connection(user, pin, host, port, dbname)
        if hashtags:
            content: list = create_post_data(hashtags, data)
            write_to_db(conn, content, table)
        #print("message={}".format(data['text']))
        time.sleep(0.5)