# Waluciak - program do pobierania kursów walut z API NBP i wczytywania ich do bazy danych za pomocą Apache Airflow 

# Wersja inicjalna: 1.0 - 24.10.2024

# Zmiany:
# 1.1 - 31.10.2024 - dodanie atrybutu ID oraz typu kursu, dodanie atrybuty z kursem vs polska złotówka
# 1.2 - 03.11.2024 - dodanie listy dla okresu dnia, przypisanie okresu dnia do poszczególnych kursów 

#TO DO:
    # zapis danych w bazie danych bezpośrednie (zejście z generowania pliku csv)
    # pobieranie kursów kategorii <> A 

# Pobieranie potrzebnych bibliotek
import requests as re
import pandas as pd
from datetime import datetime
    
# Odpytywanie API NBP o tabelę z kursami walut typu A
body = re.get("https://api.nbp.pl/api/exchangerates/tables/a/?format=json")
resp = body.json()

# Tworzenie pustych list dla data frame-u
id = [] # id 
curr = [] # nazwa waluty
code = [] # kod waluty
mid = [] # kurs waluty vs złotówka
rate_pln = [] # kurs waluty vs waluta obca 
current_date = [] # timestamp zaczytania kursu
rate_type = [] # typ kursu 
d_time_type = [] # okres dnia kursu (MON - morning; MID - midday; EOD - end of day)

# Funkcja do generowania okresów w dniu [MON, MID, EOD]
def d_time():
    if datetime.today().strftime('%H:%M%:%S') <= "10:00:00":
        d_time = "MON"
    elif datetime.today().strftime('%H:%M%:%S') >= "10:01:00" and datetime.today().strftime('%H:%M%:%S') <= "15:30:00":
        d_time = "MID"
    else:
        d_time = "EOD"
    return d_time

# Pętla do zapisu danych w poszczególnych listach
for rate in resp[0]['rates']:
    id.append(len(id) + 1)
    curr.append(rate['currency'])
    code.append(rate['code'])
    mid.append(round(rate['mid'],2))
    rate_pln.append(round(1/rate['mid'],2))
    current_date.append(datetime.today().strftime("%H:%M"))
    rate_type.append(resp[0]['table'])
    d_time_type.append(d_time())

# Przypisanie aktualnej daty do zmiennej w celu doklejenia daty do nazwy pliku
date = datetime.now().strftime("%Y%m%d")

# Powołanie data frame-u
df = pd.DataFrame(list(zip(id, curr, code, mid, rate_pln, current_date, rate_type, d_time_type))
              , columns = ['ID', 'Currency', 'Currency_Code', 'Rate_Foreign', 'Rate_PLN', 'Currency_Hour', 'Rate_Type', "Day_Time"])

# Zapis pliku csv z data frame-em na pulpicie 
local_path = '''/your_path_from_local_machine/'''
file_path = f'/{local_path}curroutput_{date}_{d_time()}.csv'

# Utworzenie pliku z rozszerzeniem csv we wskazanej lokalizacji
df.to_csv(file_path, index=False)