#from TwitterStreamAPI_v2 import Streamer
from TwitterStreamAPI_v1 import Streamer
from kafka import KafkaProducer
import json
import configparser

if __name__ == "__main__":
    config = configparser.RawConfigParser()
    config.read('config.ini')
    server_ip = config['KAFKA']['server_ip']
    bearer_token = config['TWITTER_V2']['bearer_token']
    consumer_key = config['TWITTER_V1']['consumer_key']
    consumer_secret = config['TWITTER_V1']['consumer_secret']
    OAUTH_TOKEN = config['TWITTER_V1']['OAUTH_TOKEN']
    OAUTH_TOKEN_SECRET = config['TWITTER_V1']['OAUTH_TOKEN_SECRET']
    topic = 'Tweets'

    producer = KafkaProducer(
        bootstrap_servers=[server_ip],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    # # Twitter Streaming v2 method
    # stream = Streamer(bearer_token)
    # rule = stream.get_rules()
    # stream.delete_all_rules(rule)
    # rules = [{"value": '(stock OR market OR option OR trade OR "$" OR profit OR drop OR raise OR money OR invest) lang:en'}]
    # stream.set_rules(rules)
    # stream.get_rules()
    # stream.get_stream(producer, topic)

    stream = Streamer(producer, topic,
                        consumer_key, consumer_secret, 
                        OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    stream.statuses.filter(track='uber')