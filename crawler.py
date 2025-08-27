from bs4 import BeautifulSoup
from collections import defaultdict
import logging
import requests
import sqlite3
import time
from urllib.parse import urljoin, urlparse


start = time.time()
logger = logging.getLogger(__name__)
logging.basicConfig(filename='crawler.log', level=logging.INFO)


URL = "https://www.amazon.com/"
page_cap = 10000
page = 0
update_freq = 500
crawler_depth_cap = 5
timeout = 4
crawled_sites = set()
queue = []
queue.append((URL, 0))
crawling_restrictions = defaultdict(list)

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
        
        #Robots.txt
        base = urlparse(curent_url)
        domain = f"{base.scheme}://{base.netloc}"
        if domain not in crawling_restrictions:
            robots = f"{base.scheme}://{base.netloc}/robots.txt"
            banned_sites = retreive_banned_sites(robots)
            if banned_sites is False:
                print("ERROR")
                queue.pop(0)
                continue

            crawling_restrictions[domain] += banned_sites      

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
                for banned in crawling_restrictions[domain]:
                    if str(link).startswith(banned):
                        print("             BANNED SITE")
                        break
                else:
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

#takes in a robot.txt path and returns either the banned paths or false if an error occured
def retreive_banned_sites(robots):
    try:
        request = requests.get(robots, timeout=timeout)
    except Exception as e:
        logger.info(f"requests raised an exception: {e}")
        return False

    if request.status_code != 200:
        return False
    
    lines = request.text.splitlines()
    disallowed_paths = []
    base = urlparse(robots)

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue  # skip empty lines and comments
        
        if line.lower().startswith("user-agent:"):
            current_user_agent = line.split(":")[1].strip()
        
        if current_user_agent == "*" and line.lower().startswith("disallow:"):
            path = line.split(":", 1)[1].strip()
            if path:  # only add non-empty disallow paths
                disallowed_paths.append(f"{base.scheme}://{base.netloc}{path}")
    
    return disallowed_paths

main()
end = time.time()

print(len(crawled_sites))
print(end - start)