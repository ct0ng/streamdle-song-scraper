import logging
import psycopg2
from psycopg2.extras import execute_batch

from src.config import DATABASE_CONFIG

logger = logging.getLogger(__name__)

def upsert_artist_data(artist_data):
    if not artist_data:
        return

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
        SELECT artist_id, spotify_id, name AS artist_name FROM public.artist;
    """
    cursor.execute(select_query)
    artist_data = cursor.fetchall()

    logger.info('Closing database connection')

    cursor.close()
    conn.close()

    logger.info("Fetched artist table data")

    return artist_data

def insert_album_data(album_data):
    if not album_data:
        return {}

    logger.info(f'Upserting data for {len(album_data)} albums...')

    conn = setup_database()
    cursor = conn.cursor()

    # stores mapping of (album_title, cover_url) to album_id
    album_id_map = {}

    for album_title, cover_url in album_data:
        # check if album already exists with same title and cover_url
        check_query = """
            SELECT album_id FROM public.album 
            WHERE title = %s AND (cover_url = %s OR (cover_url IS NULL AND %s IS NULL))
            LIMIT 1;
        """
        cursor.execute(check_query, (album_title, cover_url, cover_url))
        result = cursor.fetchone()
        if result:
            # use existing ID if album exists
            album_id = result[0]
        else:
            # otherwise, insert new album
            insert_query = """
                INSERT INTO public.album (title, cover_url, created_datetime)
                VALUES (%s, %s, NOW())
                RETURNING album_id;
            """
            cursor.execute(insert_query, (album_title, cover_url))
            album_id = cursor.fetchone()[0]

        album_id_map[(album_title, cover_url)] = album_id

    conn.commit()

    logger.info('Closing database connection')

    cursor.close()
    conn.close()

    logger.info("Updated album table data")

    return album_id_map

def upsert_song_data(song_data):
    if not song_data:
        return

    logger.info(f'Upserting data for {len(song_data)} songs...')

    conn = setup_database()
    cursor = conn.cursor()

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

    logger.info('Closing database connection')

    cursor.close()
    conn.close()

    logger.info("Updated song table data")

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
