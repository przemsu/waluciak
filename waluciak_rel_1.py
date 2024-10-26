#Waluciak - program do obliczania kursów walut 

#Wersja 1.0 - 26.10.2024 - Przemysław Suchan

import requests as re


def api_nbp():
    
    resp = re.get("https://api.nbp.pl/api/exchangerates/tables/a/?format=json")
    resp_json = resp.json()

    # for rate in (resp_json[0]["rates"]):
    #     if rate['code'] == 'EUR':
    #         print(rate['mid'])
    #     else:
    #         print('Waluta to nie EUR')

api_nbp()