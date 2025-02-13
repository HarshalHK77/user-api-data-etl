import requests
import json
import os
from datetime import datetime

def fetch_random_users():
    url = 'https://randomuser.me/api/?results=250'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        filename = f"./json/random_users_{datetime.now().strftime('%d_%m_%Y_%H_%M_%S')}.json"
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to {filename}")
    else:
        print('Error fetching data:', response.status_code)
        

fetch_random_users()
        
