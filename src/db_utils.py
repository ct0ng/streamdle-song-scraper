import logging
import psycopg2
from psycopg2.extras import execute_batch

from src.config import DATABASE_CONFIG

logger = logging.getLogger(__name__)

class PostgresDBConnection:
    def __init__(self):
        self.db_name = DATABASE_CONFIG['DBNAME']
        self.user = DATABASE_CONFIG['USER']
        self.password = DATABASE_CONFIG['PASSWORD']
        self.host = DATABASE_CONFIG['HOST']
        self.port = DATABASE_CONFIG['PORT']
        self.conn = None
        self.cur = None
    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cur = self.conn.cursor()
            logger.info("Database connection established successfully.")
            return self.conn, self.cur
        except Exception as e:
            logger.error(f"An error occurred while connecting to the database: {e}")
            raise
    def __exit__(self, exc_type, exc_value, exec_tb):
        if self.cur:
            self.cur.close()
        if self.conn:
            # commit any pending transactions before closing
            if exc_type is None:
                self.conn.commit()
            else:
                 # rollback if an error occurred
                self.conn.rollback()
            self.conn.close()
            logger.info("Database connection closed.")

def upsert_artist_data(artist_data):
    if not artist_data:
        return

    logger.info(f'Upserting data for {len(artist_data)} artists...')

    try:
        with PostgresDBConnection() as (conn, cursor):
            upsert_query = """
                INSERT INTO public.artist (name, spotify_id)
                VALUES (%s, %s)
                ON CONFLICT (spotify_id) DO UPDATE SET name = EXCLUDED.name;
            """
            execute_batch(cursor, upsert_query, artist_data, page_size=100)
            conn.commit()
    except Exception as e:
        logger.error(f"An error occurred while upserting artist data: {e}")
        raise

    logger.info("Successfully updated artist table data")

def query_artist_data():
    artist_data = []

    logger.info(f'Fetching data for artists...')

    try:
        with PostgresDBConnection() as (conn, cursor):
            select_query = """
                SELECT artist_id, spotify_id, name AS artist_name FROM public.artist;
            """
            cursor.execute(select_query)
            artist_data = cursor.fetchall()
    except Exception as e:
        logger.error(f"An error occurred while querying artist data: {e}")
        raise

    logger.info("Successfully fetched artist table data")

    return artist_data

def upsert_song_data(song_data):
    if not song_data:
        return

    logger.info(f'Upserting data for {len(song_data)} songs...')

    try:
        with PostgresDBConnection() as (conn, cursor):
            upsert_query = """
                INSERT INTO public.song (name, artist_id, stream_count, spotify_track_id, album_cover_url, created_datetime)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (name, artist_id) DO UPDATE SET 
                    stream_count = EXCLUDED.stream_count,
                    spotify_track_id = EXCLUDED.spotify_track_id,
                    album_cover_url = EXCLUDED.album_cover_url,
                    updated_datetime = NOW();
            """
            execute_batch(cursor, upsert_query, song_data, page_size=100)
            conn.commit()
    except Exception as e:
        logger.error(f"An error occurred while querying artist data: {e}")
        raise

    logger.info("Successfully updated song table data")
