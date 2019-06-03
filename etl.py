import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''
load_staging tables loads data to the staging tables. These are staging_events and staging_songs, which receive data directly from Amazon S3.
The function will iterate through each copy table statement in sql_queries.py:
(staging_events_copy, staging_songs_copy)
'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

'''
insert_tables inserts data into the dimensional tables from the staging tables. The dimensional tables are songplays, users, songs, artists, and time.
The create tables function will iterate through each create table statement in sql_queries.py:
(songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert)
'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

'''
This is the main function which is executed using "python etl.py" from the terminal. It should be executed after the tables are created.
'''
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()