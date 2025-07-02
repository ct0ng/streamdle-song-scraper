import requests
from bs4 import BeautifulSoup

from src.config import SCRAPER_CONFIG

artist_links = []

def scrape_artist_data():
    response = requests.get(SCRAPER_CONFIG['ARTISTS_URL'])
    soup = BeautifulSoup(response.text, 'html.parser')
    for a_tag in soup.select('table a')[:SCRAPER_CONFIG['ARTISTS_COUNT']]:
        href = a_tag.get('href')
        if href and href.startswith('/spotify/artist/'):
            full_url = SCRAPER_CONFIG['BASE_URL'] + href
            print(full_url)
            artist_links.append(full_url)
