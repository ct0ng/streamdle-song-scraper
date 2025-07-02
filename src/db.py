import psycopg2

from src.config import DATABASE_CONFIG

#TODO: refactor/clean up

conn = psycopg2.connect(
    dbname=DATABASE_CONFIG['DBNAME'],
    user=DATABASE_CONFIG['USER'],
    password=DATABASE_CONFIG['PASSWORD'],
    host=DATABASE_CONFIG['HOST'],
    port=DATABASE_CONFIG['PORT']
)

cursor = conn.cursor()
cursor.close()
conn.close()
