import logging

from src.scraper_utils import remove_dupes
from src.scraper import scrape_artist_data, scrape_song_data
from src.db_utils import upsert_artist_data, upsert_song_data

logger = logging.getLogger(__name__)
logging.basicConfig(filename='basic.log',encoding='utf-8',level=logging.INFO, filemode = 'w', format='%(process)d-%(levelname)s-%(message)s')

def main():
    artist_data = scrape_artist_data()
    cleaned_artist_data = remove_dupes(artist_data)
    upsert_artist_data(cleaned_artist_data)

    song_data = scrape_song_data()
    upsert_song_data(song_data)

if __name__ == "__main__":
    main()
