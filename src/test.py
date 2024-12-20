import datetime
import requests

url = "https://api.upbit.com/v1/candles/days"

params = {
    'market': 'USDT-XRP',
    'count': 1,
    'to': '2024-12-17T00:00:00'
}

headers = {"accept": "application/json"}
response = requests.get(url, params=params, headers=headers)

data = response.json()
print(data)