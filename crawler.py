from urllib.request import urlopen
import urllib
from pymongo import MongoClient
from bs4 import BeautifulSoup

DB_HOST = 'localhost:27017'   
try:
    client = MongoClient(host=[DB_HOST])
    db = client.hw3
    pagecollection = db['pages']
except:
    print("Database not connected")
    
class Frontier:
    
    def __init__(self):
        self.urls = []
        self.visited = set()
        
    def finish(self):
        return len(self.urls) == 0
    
    def next(self):
        return self.urls.pop(0)
    
    def addurl(self, url):
        if url not in self.visited and url not in self.urls:
            self.urls.append(url)
            
    def clearfront(self):
        self.urls.clear()

def retrieveurl(url, baseurl='https://www.cpp.edu/sci/computer-science/'):
    try:
        furl = urllib.parse.urljoin(baseurl, url)
        with urllib.request.urlopen(furl) as response:
            html = response.read()
            print("it worked")
        return html
    except:
        print("it didnt work")
        
def storepage(url, html):
    if html:
        pagedata = {'url': url, 'html': html.decode('utf-8')}
        pagecollection.insert_one(pagedata)
   
def targetpage(html):
    bs = BeautifulSoup(html, 'html.parser')
    heading = bs.find("h1", string="Permanent Faculty")
    return heading 

def parse(html):
    bs = BeautifulSoup(html, 'html.parser')
    links = bs.find_all('a', href=True)
    hrefs = []
    for link in links:
        href = link.get('href')
        if href:
            hrefs.append(href)
    return hrefs
        
frontier = Frontier()
frontier.addurl('https://www.cpp.edu/sci/computer-science/')
       
while not frontier.finish():
    url = frontier.next()
    html = retrieveurl(url)
    storepage(url, html)

    if targetpage(html):
        frontier.clearfront()
    else:
        for next in parse(html):
            frontier.addurl(next)