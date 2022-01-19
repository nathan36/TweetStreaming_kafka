from twython import Twython
from twython import TwythonStreamer
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
consumer_key = config['TWITTER']['api_key']
consumer_secret = config['TWITTER']['api_secret']

twitter = Twython(consumer_key, consumer_secret)
auth = twitter.get_authentication_tokens(callback_url='oob')
OAUTH_TOKEN = auth['oauth_token']
OAUTH_TOKEN_SECRET = auth['oauth_token_secret']
print(auth['auth_url'])
verifier = input('Please enter PIN')
twitter = Twython(consumer_key, consumer_secret,
                  OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
final_step = twitter.get_authorized_tokens(verifier)
OAUTH_TOKEN = final_step['oauth_token']
OAUTH_TOKEN_SECRET = final_step['oauth_token_secret']

class Streamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
            print(data['text'])

    def on_error(self, status_code, data, header=None):
        print(status_code)

if __name__ == '__main__':
    stream = Streamer(consumer_key, consumer_secret,
                        OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    stream.statuses.filter(track='$UBER')



