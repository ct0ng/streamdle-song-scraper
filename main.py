
from src.scraper import scrape_artist_data
from src.db_utils import insert_artist_data

def main():
    # scrape artist data
    artist_data = scrape_artist_data()
    insert_artist_data(artist_data)

if __name__ == "__main__":
    main()
