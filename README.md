# Project Summary:

- A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

- As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.


# How to Run Python Scripts:

- First, provision a Redshift cluster.

- Next, populate the dwh.cfg configuration file with the necessary parameters from the cluster, IAM role, and S3 bucket.

- Following that, run create_tables.py to connect to the Redshift cluster, drop any tables that already exist, and create the staging_events, staging_songs, factSongplay, dimUsers, dimSongs, dimArtists, and dimTime tables.

- After that, run the etl.py script to copy all log and song JSON files into the staging tables, then insert the data into the fact and dimension tables in the Redshift database.

- Note: The sql_queries.py script contains queries used by create_tables.py and etl.py for SQL queries. This does not need to be run by the user, as it is imported by the other Python files.


# Explanation of Files in this Repository:

### 1) create_tables.py

- This script connects to the Sparkify database, drops any table that already exits using the function drop_tables(), and creates the tables using the function create_tables().

### 2) dwh.cfg

- This is a configuration file that holds parameters necessary for connecting to the Redshift cluster, the
IAM role Amazon Resource Name (ARN), and the directory details for the S3 bucket that holds the data.

### 3) etl.py

- This script connects to the Sparkify database. It contains a function called load_staging_tables(), which loads data from log_data and song_data in the S3 bucket into the Redshift staging tables. It also has the function called insert_tables() that is used for inserting data from the staging tables into the final fact and dimension tables.

### 4) sql_queries.py

- This script creates lists of the queries that are used in create_tables.py. Each element of these lists contains the details for the columns in each table.
