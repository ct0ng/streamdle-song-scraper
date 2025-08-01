import logging
import requests
from bs4 import BeautifulSoup

from src.db_utils import query_artist_data
from src.config import SCRAPER_CONFIG, SONG_RANGES

ARTISTS_URL = SCRAPER_CONFIG['ARTISTS_URL']
BASE_URL = SCRAPER_CONFIG['BASE_URL']
ARTISTS_COUNT = SCRAPER_CONFIG['ARTISTS_COUNT']

logger = logging.getLogger(__name__)

def scrape_artist_data():

    artist_data = []

    logger.info(f'Scraping data for {ARTISTS_COUNT} artists...')

    response = requests.get(ARTISTS_URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    for a_tag in soup.select('table a')[:ARTISTS_COUNT]:
        href = a_tag.get('href')
        name = a_tag.text.strip()
        if href and href.startswith('/spotify/artist/'):
            spotify_id = href.split("/")[-1].replace("_songs.html", "")
            artist_data.append((name, spotify_id))

    logger.info(f'Successfully scraped data for {len(artist_data)} artists')

    return artist_data

def scrape_song_data(): 
    
    song_data = []

    logger.info(f'Scraping song data...')

    artist_data = query_artist_data()
    for idx, (artist_id, spotify_id) in enumerate(artist_data):
        songs_url = f"{BASE_URL}/{spotify_id}_songs.html"
        try:
            response = requests.get(songs_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            rows = []
            row_limit = next((limit for r, limit in SONG_RANGES.items() if idx in r), 25)
            rows = soup.select("table")[1].select("tr")[1:row_limit]
            for row in rows:
                cols = row.find_all("td")
                title = cols[0].text.strip()
                # ignore songs where the artist is a feature
                if not title.startswith('* '):
                    streams_str = cols[1].text.strip().replace(",", "")
                    if streams_str.isdigit():
                        stream_count = int(streams_str)
                        song_data.append((title, artist_id, stream_count))
                    else:
                        continue

            logger.info(f'Total songs scraped so far: {len(song_data)}')

        except Exception as e:
            logger.info(f"Error scraping {songs_url}: {e}")

    logger.info(f'Successfully scraped data for {len(song_data)} songs')

    return song_data