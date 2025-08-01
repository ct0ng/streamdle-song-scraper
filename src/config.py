from dotenv import load_dotenv
import os

load_dotenv() 

DATABASE_CONFIG = {
    'HOST': os.getenv("DB_HOST"),
    'DBNAME': os.getenv("DB_NAME"),
    'PORT': os.getenv("DB_PORT"),
    'USER': os.getenv("DB_USER"),
    'PASSWORD': os.getenv("DB_PASSWORD"),
}

SCRAPER_CONFIG = {
    'ARTISTS_COUNT': 3000,
    'BASE_URL': 'https://kworb.net/spotify/artist',
    'ARTISTS_URL': 'https://kworb.net/spotify/artists.html'
}

# Define the ranges for the number of songs to scrape based on the artist's total stream ranking
# E.g. an artist that's in the top 100 will have 25 songs scraped
SONG_RANGES = {
    range(2000, 3000): 5,
    range(1000, 2000): 10,
    range(500, 1000): 15,
    range(100, 500): 20,
    range(0, 100): 25,
}
