import json
import time
import random
from _datetime import datetime
from StockTwitsAPI import stream_symbol
from kafka import KafkaProducer
import configparser

def serializer(message):
    return json.dumps(message).encode('utf-8')

def get_tweets(stocks: list, topic: str, producer: KafkaProducer):
    for stock in stocks:
        res = stream_symbol(stock)
        if res:
            if res['response']['status'] == 200:
                message = res['messages']
                print_out = message[0]['body']
                print(f'Producing message @ {datetime.now()} | Message = {str(print_out)}')
                producer.send(topic, message)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    server_ip = config['KAFKA']['server_ip']
    stocks = ['UBER']
    topic = 'test'

    producer = KafkaProducer(
        bootstrap_servers=[server_ip],
        value_serializer=serializer
    )

    while True:
        get_tweets(stocks, topic, producer)
        time_to_sleep = random.randint(1,11)
        time.sleep(time_to_sleep)