import psycopg2
from psycopg2.extras import execute_batch

from src.config import DATABASE_CONFIG

def insert_artist_data(artist_data):

    print(f'Inserting data for {len(artist_data)} artists...')
    print('Opening database connection')

    conn = psycopg2.connect(
        dbname=DATABASE_CONFIG['DBNAME'],
        user=DATABASE_CONFIG['USER'],
        password=DATABASE_CONFIG['PASSWORD'],
        host=DATABASE_CONFIG['HOST'],
        port=DATABASE_CONFIG['PORT']
    )

    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO public.artist (name, spotify_id)
        VALUES (%s, %s)
        ON CONFLICT (spotify_id) DO UPDATE SET name = EXCLUDED.name;
    """
    execute_batch(cursor, upsert_query, artist_data, page_size=100)
    conn.commit()

    print('Closing database connection')

    cursor.close()
    conn.close()

    print("Updated artist table data")
