from bs4 import BeautifulSoup
from pymongo import MongoClient
import re

DB_HOST = 'localhost:27017'   
try:
    client = MongoClient(host=[DB_HOST])
    db = client.hw3
    pagecollection = db['pages']
    professorcollection = db['professors']
except:
    print("Database not connected")

page = pagecollection.find(filter={"url": "/sci/computer-science/faculty-and-staff/permanent-faculty.shtml"}, projection={"html": 1, "_id": 0})
html = page[0]["html"]

bs = BeautifulSoup(html, 'html.parser')
profs = bs.find_all('div', class_='clearfix')
pdata = []

for professor in profs:
    pname = professor.find('h2')
    if pname:
        pnameget = pname.get_text()
        ptitle = professor.find('strong', string=re.compile('.*Title.*')).next_sibling.get_text()
        poffice = professor.find('strong', string=re.compile('.*Office.*')).next_sibling.get_text()
        pemail = professor.find('strong', string=re.compile('.*Email.*')).find_next('a').get('href').split(':')[1]
        pwebsite = professor.find('strong', string=re.compile('.*Web.*')).find_next('a').get('href')
        play = {
        'Professor Name': pnameget,
        'Title': ptitle,
        'Office': poffice,
        'Email': pemail,
        'Website': pwebsite
        }
        pdata.append(play)
        
if pdata:
    professorcollection.insert_many(pdata)