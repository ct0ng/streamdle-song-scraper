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
    'BASE_URL': "https://kworb.net",
    'ARTISTS_URL': "https://kworb.net/spotify/artists.html",
    'ARTISTS_COUNT': 3000,
}
