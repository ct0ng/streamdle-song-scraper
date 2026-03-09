import logging

from src.scraper_utils import remove_dupes
from src.scraper import scrape_artist_data, scrape_song_and_album_data
from src.db_utils import upsert_artist_data, upsert_song_data, insert_album_data

logger = logging.getLogger(__name__)
logging.basicConfig(filename='scraper.log',encoding='utf-8',level=logging.INFO, filemode = 'w', format='%(process)d-%(levelname)s-%(message)s')

def main():
    """
    Main entry point for entire Python script.
    This function performs the following steps:
        1. Scrapes artist data
        2. Cleans and removes duplicate artist data
        3. Upserts the cleaned artist data into the database
        4. Scrapes song and album information
        5. Upserts albums into the album table
        6. Maps songs to albums and upserts song data with album_id
    """
    artist_data = scrape_artist_data()
    cleaned_artist_data = remove_dupes(artist_data)

    upsert_artist_data(cleaned_artist_data)

    song_data, album_data = scrape_song_and_album_data()

    album_id_map = insert_album_data(album_data)

    # map songs to album_ids and prepare final song data
    final_song_data = []
    for title, artist_id, stream_count, album_title, cover_url in song_data:
        album_id = None
        if album_title:
            # look up album_id by album_title and cover_url
            album_id = album_id_map.get((album_title, cover_url))

        final_song_data.append((title, artist_id, stream_count, album_id))

    upsert_song_data(final_song_data)

if __name__ == "__main__":
    main()
