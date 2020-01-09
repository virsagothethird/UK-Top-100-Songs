from pymongo import MongoClient
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random


url = 'https://www.officialcharts.com/charts/'

uk_db = pd.DataFrame()

client = MongoClient('localhost', 27017)
db = client.uk100
pages = db.main_pages

num = 0

# Takes the raw beautiful soup data, transforms it and appends to a pandas df
def extract_data(start, stop, steps=5):
    for i in range(start, stop,steps):    
            row = tags[i]
            row = row.text.split('\n')
            while("" in row):
                row.remove("")
            row.append(date)
            uk_db = uk_db.append([row])

            
# Scrapes the url and extracts html information and transforms it into a usable csv
def scrape_it(urls, num_weeks):

    for i in range(num_weeks):

        num += 1

        r = requests.get(url)

        soup = BeautifulSoup(r.text, 'html.parser')
        tags= soup.find_all('tr')

        date = soup.find('p', 'article-date')
        date = date.text.split('\n')
        date = date[1].split()
        date = '{} {} {}'.format(date[1], date[0], date[2])

        pages.insert_one({'html{}'.format(num): r.content})


        extract_data(1, 147, steps=5)
        extract_data(152, 298, steps=5)
        extract_data(303, 450, steps=5)
        extract_data(454, 503, steps=5)
        

        url_find = soup.find_all('a', 'prev chart-date-directions', href=True)
        prev_url = url_find[0]['href']
        url = 'https://www.officialcharts.com' + prev_url

        time.sleep(random.randint(6,9))


    uk_db.columns = ['rank', 'last_week_rank', 'hmm', 'title', 'artist', 'label', 'peak_rank', 'weeks_on_chart', 'buy', 'listen', 'week_of']
    uk_db.reset_index(inplace=True)
    uk_db = test_db5.drop(['buy', 'listen', 'index'], axis=1)
    uk_db = test_db5.bfill().ffill()
    uk_db.to_csv('uk100.csv')
    

if __name__ == "__main__":
    scrape_it(url, 260)