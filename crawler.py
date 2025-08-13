from bs4 import BeautifulSoup
import logging
import requests
import sqlite3
import time
from urllib.parse import urljoin


start = time.time()
logger = logging.getLogger(__name__)
logging.basicConfig(filename='crawler.log', level=logging.INFO)


URL = "https://bulbapedia.bulbagarden.net/wiki/Comet_Shard"
page_cap = 100000
page = 0
update_freq = 500
crawler_depth_cap = 5
timeout = 4
crawled_sites = set()
queue = []
queue.append((URL, 0))

def main():
    global page

    conn = sqlite3.connect('search_engine.db')
    cursor = conn.cursor()

    while (len(queue) > 0):
        curent_url, depth = queue[0]
        print(curent_url, depth)

        #depth cap
        if depth >= crawler_depth_cap:
            queue.pop(0)
            continue
        #page cap
        if page >= page_cap:
            print("PAGECAP")
            break

        #connect to site
        if(curent_url not in crawled_sites):
            try:
                resp = requests.get(curent_url, timeout=timeout)
            except Exception as e:
                logger.info(f"requests raised an exception: {e}")
                queue.pop(0)
                continue
            
            #get all links
            html = BeautifulSoup(resp.text, 'html.parser')
            for link in html.find_all('a'):
                link_to_queue(link, curent_url, depth)

        
        #mark as crawled        
        crawled_sites.add(curent_url)
        cursor.execute("INSERT OR IGNORE INTO URLs (url, indexed) VALUES (?, ?)", 
                (curent_url, 0))

        #periodically commit
        if (page % update_freq) == 0:
            conn.commit()

        
        page +=1 
        queue.pop(0)
    
    conn.close()


def link_to_queue(link, curent_url, depth):
    try:
        link = urljoin(curent_url,link.get('href'))
        link = link.split("#")[0]

        if not (link.startswith("http://") or link.startswith("https://")):
            return

        queue.append((link, depth +1))
    except Exception as e:
        logger.info(f"Link to queue raised an exception: {e}")

main()
end = time.time()

print(len(crawled_sites))
print(end - start)