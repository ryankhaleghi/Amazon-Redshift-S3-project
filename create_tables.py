import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

'''
drop_tables drops all of the tables.
The function will iterate through each drop table statement in sql_queries.py:
(staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop)
'''
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

'''
create_tables creates all of the tables. 
staging_events and staging_songs will get data directly from S3. The dimensional tables will receive data from the staging_events and staging_songs tables.
The create tables function will iterate through each create table statement in sql_queries.py:
(staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create)
'''
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


'''
This is the main function which is executed using "python create_tables.py" from the terminal. It should be executed before etl.py.
'''     
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()