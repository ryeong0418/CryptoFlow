import json
import requests
import datetime
from Scripts.common.utils import SystemUtils
from dotenv import load_dotenv
import os


class CryptoExtractor():

    def __init__(self):
        load_dotenv()
        self.all_market = 'https://api.upbit.com/v1/market/all'
        self.candles_days = 'https://api.upbit.com/v1/candles/days'
        self.conn_str = os.getenv('CONNECTION_STRING')

    def get_market_list(self):

        market_list= []
        headers = {'accept':'application/json'}
        response = requests.get(self.all_market, headers=headers)
        data = response.json()

        for i in data:
            if i['korean_name'] in ['이더리움', '비트코인', '리플']:
                market_list.append(i['market'])

        return market_list

    def mapping_market_container(self, data_json, changed_date, market):
        '''

        :param data_json:
        :param changed_date:
        :param market:
        :return:

        dictionary mapping을 통해 중복된 코드를 줄이고 간결하게 표시
        '''

        market_to_container={
                                'KRW-BTC': 'historicaldata-krw-btc',
                                'KRW-ETH': 'historicaldata-krw-eth',
                                'BTC-ETH': 'historicaldata-btc-eth',
                                'BTC-XRP': 'historicaldata-btc-xrp',
                                'USDT-BTC': 'historicaldata-usdt-btc',
                                'USDT-ETH': 'historicaldata-usdt-eth',
                                'USDT-XRP': 'historicaldata-usdt-xrp',
                                'KRW-XRP': 'historicaldata-krw-xrp'

                            }

        if market in market_to_container.keys():
            SystemUtils.upload_to_blob_storage(market, changed_date, self.conn_str, market_to_container[market], data_json)

    def get_candle_days_data(self, market_list):

        for market in market_list:
            market_data=[]
            param_date = datetime.datetime.now()
            days_to_collect = 5  # 5년치 데이터 (365일 * 5)
            days_per_request = 1  # API 요청당 가져올 데이터 수
            print(market)

            while len(market_data) < days_to_collect:

                try:

                    params = {
                        'market': market,
                        'count': days_per_request,
                        'to': param_date.strftime("%Y-%m-%d %H:%M:%S")
                     }

                    headers = {"accept": "application/json"}
                    response = requests.get(self.candles_days, params=params, headers=headers)

                    if response.status_code != 200:
                        raise ValueError(f"HTTP Error: {response.status_code} - {response.reason}")

                    data = response.json()
                    market_data.append(data)
                    oldest_date_str = data[-1]['candle_date_time_utc']

                    param_date = datetime.datetime.strptime(oldest_date_str, '%Y-%m-%dT%H:%M:%S') - datetime.timedelta(seconds=1)

                    changed_date = oldest_date_str.split('T')[0]

                    data_json = json.dumps(data, indent=4, sort_keys=True, ensure_ascii=False)
                    CryptoExtractor.mapping_market_container(data_json, changed_date, market)

                except requests.exceptions.RequestException as e:
                    print(f"Network error occurred: {str(e)}")

                except json.JSONDecodeError as e:
                    print(f"JSON decoding error: {str(e)}. Response content: {response.text}")

                except KeyError as e:
                    print(f"KeyError: {str(e)}. Response data: {data}")

                except ValueError as e:
                    print(f"ValueError: {str(e)}")

                except Exception as e:
                    print(f"An unexpected error occurred: {str(e)}")

crypt = CryptoExtractor()
# market_list = crypt.get_market_list()
# CryptoExtractor.get_candle_days_data(market_list)
