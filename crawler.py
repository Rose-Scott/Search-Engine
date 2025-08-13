from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import time
import sqlite3


conn = sqlite3.connect('search_engine.db')
cursor = conn.cursor()
start = time.time()


URL = "https://www.geeksforgeeks.org/python/queue-in-python/"
crawler_depth_cap = 2
timeout = 4
crawled_sites = set()
queue = []
queue.append((URL, 0))


while (len(queue) > 0):
    curent_url, depth = queue[0]
    print(curent_url, depth)

    #depth cap
    if depth >= crawler_depth_cap:
        queue.pop(0)
        continue

    if(curent_url not in crawled_sites):
        try:
            resp = requests.get(curent_url, timeout=timeout)
        except requests.exceptions.Timeout:
            queue.pop(0)
            continue

        html = BeautifulSoup(resp.text, 'html.parser')


        crawled_sites.add(curent_url)
        cursor.execute("INSERT OR IGNORE INTO URLs (url, indexed) VALUES (?, ?)", 
               (curent_url, 0))


        for link in html.find_all('a'):
            link = urljoin(curent_url,link.get('href'))
            link = link.split("#")[0]

            if not (link.startswith("http://") or link.startswith("https://")):
                continue

            queue.append((link, depth +1))

    queue.pop(0)
    


conn.commit()
conn.close()

end = time.time()

print(len(crawled_sites))
print(end - start)