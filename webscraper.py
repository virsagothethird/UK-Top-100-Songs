from pymongo import MongoClient
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random


url = 'https://www.officialcharts.com/charts/'

test_db5 = pd.DataFrame()

client = MongoClient('localhost', 27017)
db = client.uk100
pages = db.main_pages

num = 0

for i in range(260):

    num += 1

    r = requests.get(url)
        
    soup = BeautifulSoup(r.text, 'html.parser')
    tags= soup.find_all('tr')
    
    date = soup.find('p', 'article-date')
    date = date.text.split('\n')
    date = date[1].split()
    date = '{} {} {}'.format(date[1], date[0], date[2])
    
    pages.insert_one({'html{}'.format(num): r.content})
        
    
    
    for i in range(1, 147,5):    
        row = tags[i]
        row = row.text.split('\n')
        while("" in row):
            row.remove("")
        row.append(date)
        test_db5 = test_db5.append([row])

    for i in range(152, 298,5):
        row = tags[i]
        row = row.text.split('\n')
        while("" in row):
            row.remove("")
        row.append(date)
        test_db5 = test_db5.append([row])

    for i in range(303, 450,5):
        row = tags[i]
        row = row.text.split('\n')
        while("" in row):
            row.remove("")
        row.append(date)
        test_db5 = test_db5.append([row])

    for i in range(454, 503,5):
        row = tags[i]
        row = row.text.split('\n')
        while("" in row):
            row.remove("")
        row.append(date)
        test_db5 = test_db5.append([row])
        
    url_find = soup.find_all('a', 'prev chart-date-directions', href=True)
    prev_url = url_find[0]['href']
    url = 'https://www.officialcharts.com' + prev_url
    
    time.sleep(random.randint(6,9))


test_db5.columns = ['rank', 'last_week_rank', 'hmm', 'title', 'artist', 'label', 'peak_rank', 'weeks_on_chart', 'buy', 'listen', 'week_of']
test_db5.reset_index(inplace=True)
test_db5 = test_db5.drop(['buy', 'listen', 'index'], axis=1)
test_db5 = test_db5.bfill().ffill()
test_db5.to_csv('uk100.csv')