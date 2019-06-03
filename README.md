Amazon Redshift Project

The purpose of this project is to help a fictional music streaming startup, Sparkify, move their processes into the cloud. The data is in S3 buckets as JSON logs of user activity, and JSON metadata for the songs. This represents all the songs on the app, and all the user data of what songs they played and when. THe goal is to find insights about what songs users are listening to.


Schema
The data is stored in song files and log files in an Amazon S3 bucket. The song and log files are JSON files. The first step in the ETL pipeline is to read the JSON files, and copy the data directly into two staging tables, staging_songs and staging_events, respectfully. 

The staging_songs table contains information about each song that is available to play on the Sparkify service. The staging_events table contains the user activity data on the Sparkify service.

Once the data is in the staging tables, the second step in the ETL pipeline is then to take that data and insert it into the dimensional tables: users, artists, songs, time, and songplays. The scheme for the dimensional tables is a snowflake, with songsplays as the fact table and the others as the dimension tables. This schema will allow analysts to query data to perform analytics on the Sparkify data efficiently.


Files 

dwh.cfg:
This is the configuration file which contains the information necessary to connect to the Amazon S3 and Redshift services. This includes the host (HOST), database (DB_NAME) username (DB_USER), password (DB_PASSWORD), and port (PORT) to connect to the cluster. HOST should be enclosed in single quotes, but not the other variables. The IAM role information on Amazon Redshfit should be copied to the ARN field under IAM_ROLE. Under S3 are the paths to the S3 buckets for the song (SONG_DATA) and log data (LOG_DATA) JSON files. The LOG_JSONPATH is the path to the configuration file that defines the JSON datatype used for the log files. 

create_tables.py
This file contains the functions for creating and dropping the staging and dimensional tables. It contains two functions, drop_tables and create_tables that will be executed upon running create_tables.py. drop_tables will be executed before create_tables.


etl.py
This file contains the functions for putting data into the staging and dimensional tables. It contains two functions, load_staging_tables and insert_tables that will be executed upon running etl.py. The load_staging_tables function will be executed first, and will copy data from the Amazon S3 bucket into the staging tables. The insert_tables function will run second, and pull data from the staging tables and insert it into the dimensional tables.

sql_queries.py
This file contains all of the SQL queries that are executed by create_tables.py and etl.py. 
This includes dropping tables, creating tables, copying data to the staging tables, and inserting data into the dimensional tables. This file is not run directly.


EXECUTION

In the Terminal, enter "python create_tables.py" without the quotes to drop the existing tables and create new tables. Then, enter "python etl.py" to pull data into the staging tables, and then pull data into the dimensional tables.


