import requests
import json

def stream_symbol(symbol):
    url = "https://api.stocktwits.com/api/2/streams/symbol/" + str(symbol) + ".json"
    content = requests.get(url).text
    return json.loads(content)

#testing
# symbol = 'UBER'
# res = stream_symbol(symbol)
# print(res['messages'])


