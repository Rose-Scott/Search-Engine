from bs4 import BeautifulSoup
import re
import requests
import sqlite3
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='indexer.log', level=logging.INFO)
#sql setup
conn = sqlite3.connect('search_engine.db')
rows_cursor = conn.cursor()
update_cursor = conn.cursor()

#Request setup
timeout = 3




for row in rows_cursor.execute("SELECT url, url_id FROM URLs WHERE indexed IS FALSE"):
    print("ROW")
    URL = row[0]
    url_id = row[1]
    print(url_id)

    #html reqeuest
    try:
        html = requests.get(URL, timeout=timeout).text
    except Exception as e:
        logger.info(f"requests raised an exception: {e}")
        continue

    
    #Clean text
    text = BeautifulSoup(html, 'html.parser')
    for tag in text(['script', 'style', 'noscript', 'meta', 'header', 'footer', 'nav']):
        tag.decompose()

    text = text.get_text(separator=' ')
    text = re.sub(r'\s+', ' ', text).strip()
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    tokens = text.split()

    for token in tokens:
        update_cursor.execute("INSERT OR IGNORE INTO terms (term) VALUES (?)", 
               (token,))
        
        update_cursor.execute("SELECT term_id FROM terms WHERE term = ?", (token,))
        term_id = update_cursor.fetchone()[0]
        
        update_cursor.execute('''
            INSERT INTO index_entries (term_id, url_id, frequency)
            VALUES (?, ?, 1)
            ON CONFLICT(term_id, url_id) 
            DO UPDATE SET frequency = frequency + 1
            ''', (term_id, url_id))
        
    update_cursor.execute("UPDATE URLs SET indexed = 1 WHERE url_id = ?", (url_id,))
    conn.commit()    

update_cursor.execute("SELECT * FROM index_entries")
rows = update_cursor.fetchall()



conn.close()
        
        

