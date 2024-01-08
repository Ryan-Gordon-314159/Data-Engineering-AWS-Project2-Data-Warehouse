import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """Loads data from the Amazon S3 bucket into the Redshift staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """Inserts data into the final fact and dimension tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """Uses parameters from the dwh.cfg configuration file to connect to the postgreSQL database and   
    creates cursor that allows for queries to be executed."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()