import requests
from bs4 import BeautifulSoup

from src.config import SCRAPER_CONFIG

BASE_URL = 'https://kworb.net'
ARTISTS_URL = 'https://kworb.net/spotify/artists.html'

def scrape_artist_data():

    artist_data = []

    print(f'Scraping {SCRAPER_CONFIG['ARTISTS_COUNT']} artist data...')

    response = requests.get(ARTISTS_URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    for a_tag in soup.select('table a')[:SCRAPER_CONFIG['ARTISTS_COUNT']]:
        href = a_tag.get('href')
        name = a_tag.text.strip()
        if href and href.startswith('/spotify/artist/'):
            spotify_id = href.split("/")[-1].replace("_songs.html", "")
            # full_url = BASE_URL + href
            # print(full_url)
            artist_data.append((name, spotify_id))

    print(f"Successfully scraped {len(artist_data)} artist data entries")

    return artist_data
