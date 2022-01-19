import json
from kafka import KafkaConsumer
import configparser

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    server_ip = config['KAFKA']['server_ip']
    topic = 'StockTwits'
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server_ip,
        auto_offset_reset='earliest'
    )
    for message in consumer:
        print(json.loads(message.value))