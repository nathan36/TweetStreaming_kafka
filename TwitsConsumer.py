import json
from kafka import KafkaConsumer


if __name__ == '__main__':
    topic = 'StockTwits'
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='ec2-18-237-169-51.us-west-2.compute.amazonaws.com:9092',
        auto_offset_reset='earliest'
    )
    for message in consumer:
        print(json.loads(message.value))