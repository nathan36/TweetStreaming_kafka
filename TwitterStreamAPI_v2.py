import requests
import json
from dataclasses import dataclass
from datetime import datetime
import random
import time

@dataclass
class Streamer:
    bearer_token: str

    def _bearer_oauth(self, r: requests) -> requests:
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    def delete_all_rules(self, rules: json):
        if rules is None or "data" not in rules:
            return None

        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self._bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print(json.dumps(response.json()))

    def get_rules(self) -> json:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self._bearer_oauth
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))
        return response.json()

    def set_rules(self, rules: list) -> None:
        payload = {"add": rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self._bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )

    def get_stream(self, producer, topic) -> None:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", auth=self._bearer_oauth, stream=True,
        )
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                print(f'Producing message @ {datetime.now()} | Message = {str(json_response)}')
                producer.send(topic, json_response)
                time_to_sleep = random.randint(1, 5)
                time.sleep(time_to_sleep)


#testing
# import configparser
#
# config = configparser.ConfigParser()
# config.read('config.ini')
# server_ip = config['KAFKA']['server_ip']
# bearer_token = config['TWITTER']['bearer_token']
# topic = 'StockTwits'
#
# stream = Streamer(bearer_token)
# rule = stream.get_rules()
# stream.delete_all_rules(rule)
# rules = [{"value": 'Uber'}]
# stream.set_rules(rules)
# producer = KafkaProducer(
#     bootstrap_servers=[server_ip],
#     value_serializer= lambda m: json.dumps(m).encode('utf-8')
# )
# stream.get_stream(producer, topic)

