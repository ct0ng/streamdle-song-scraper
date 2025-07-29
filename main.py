
from src.scraper_utils import remove_dupes
from src.scraper import scrape_artist_data
from src.db_utils import insert_artist_data

def main():
    artist_data = scrape_artist_data()
    cleaned_artist_data = remove_dupes(artist_data)
    insert_artist_data(cleaned_artist_data)

if __name__ == "__main__":
    main()
