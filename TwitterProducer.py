from TwitterStreamAPI_v2 import Streamer
from kafka import KafkaProducer
import json
import configparser

if __name__ == "__main__":
    config = configparser.RawConfigParser()
    config.read('config.ini')
    server_ip = config['KAFKA']['server_ip']
    bearer_token = config['TWITTER']['bearer_token']
    topic = 'Tweets'

    stream = Streamer(bearer_token)
    rule = stream.get_rules()
    stream.delete_all_rules(rule)
    rules = [{"value": '(stock OR market OR option OR trade OR "$" OR profit OR drop OR raise OR money OR invest) lang:en'}]
    stream.set_rules(rules)
    stream.get_rules()
    producer = KafkaProducer(
        bootstrap_servers=[server_ip],
        value_serializer= lambda m: json.dumps(m).encode('utf-8')
    )
    stream.get_stream(producer, topic)