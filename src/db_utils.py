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
            print("Database connection closed.")

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

def insert_album_data(album_data):
    if not album_data:
        return {}

    logger.info(f'Upserting data for {len(album_data)} albums...')

    # stores mapping of (album_title, cover_url) to album_id
    album_id_map = {}

    try:
        with PostgresDBConnection() as (conn, cursor):
            for album_title, cover_url in album_data:
                check_query = """
                    SELECT album_id FROM public.album 
                    WHERE title = %s AND (cover_url = %s OR (cover_url IS NULL AND %s IS NULL))
                    LIMIT 1;
                """
                cursor.execute(check_query, (album_title, cover_url, cover_url))
                result = cursor.fetchone()
                if result:
                    album_id_map[(album_title, cover_url)] = result[0]
                else:
                    insert_query = """
                        INSERT INTO public.album (title, cover_url, created_datetime)
                        VALUES (%s, %s, NOW())
                        RETURNING album_id;
                    """
                    cursor.execute(insert_query, (album_title, cover_url))
                    album_id = cursor.fetchone()[0]
                    album_id_map[(album_title, cover_url)] = album_id
    except Exception as e:
        logger.error(f"An error occurred while checking/inserting album data: {e}")
        raise

    logger.info("Successfully updated album table data")

    return album_id_map

def upsert_song_data(song_data):
    if not song_data:
        return

    logger.info(f'Upserting data for {len(song_data)} songs...')

    try:
        with PostgresDBConnection() as (conn, cursor):
            upsert_query = """
                INSERT INTO public.song (title, artist_id, stream_count, album_id, created_datetime)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (title, artist_id) DO UPDATE SET 
                    stream_count = EXCLUDED.stream_count, 
                    album_id = EXCLUDED.album_id,
                    updated_datetime = NOW();
            """
            execute_batch(cursor, upsert_query, song_data, page_size=100)
            conn.commit()
    except Exception as e:
        logger.error(f"An error occurred while querying artist data: {e}")
        raise

    logger.info("Successfully updated song table data")
