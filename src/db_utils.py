import logging
import psycopg2
from psycopg2.extras import execute_batch

from src.config import DATABASE_CONFIG

logger = logging.getLogger(__name__)

def upsert_artist_data(artist_data):

    logger.info(f'Upserting data for {len(artist_data)} artists...')

    conn = setup_database()
    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO public.artist (name, spotify_id)
        VALUES (%s, %s)
        ON CONFLICT (spotify_id) DO UPDATE SET name = EXCLUDED.name;
    """
    execute_batch(cursor, upsert_query, artist_data, page_size=100)
    conn.commit()

    logger.info('Closing database connection')

    cursor.close()
    conn.close()

    logger.info("Updated artist table data")

def query_artist_data():

    artist_data = []

    logger.info(f'Fetching data for artists...')
    
    conn = setup_database()
    cursor = conn.cursor()

    select_query = """
        SELECT artist_id, spotify_id FROM public.artist;
    """
    cursor.execute(select_query)
    artist_data = cursor.fetchall()

    logger.info('Closing database connection')

    cursor.close()
    conn.close()

    logger.info("Fetched artist table data")

    return artist_data

def setup_database():
    logger.info('Opening database connection')

    conn = psycopg2.connect(
        dbname=DATABASE_CONFIG['DBNAME'],
        user=DATABASE_CONFIG['USER'],
        password=DATABASE_CONFIG['PASSWORD'],
        host=DATABASE_CONFIG['HOST'],
        port=DATABASE_CONFIG['PORT']
    )

    return conn
