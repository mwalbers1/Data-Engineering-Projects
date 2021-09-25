import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def define_search_path(cur, conn):
    """
    Set search path to music schema
    
    Args:
    cur: cursor object for executing database query
    conn: database connection object
    
    Returns: None
    """
    
    name = "music"
    query = "SET search_path TO {};".format(name)
    cur.execute(query)
    conn.commit()
    

def load_staging_tables(cur, conn):
    """
    Load AWS S3 json songs and event data into staging tables
    
    Args:
    cur: cursor object for executing database query
    conn: database connection object
    
    Returns: None
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data into dimension tables and fact table from staging tables
    
    Args:
    cur: cursor object for executing database query
    conn: database connection object
    
    Returns: None
    """
    
    for query in insert_table_queries:
        print(f"Inserting {query[:50]} ..")
        cur.execute(query)
        conn.commit()


def main():
    """
    Called from command line as: python etl.py
    
    - Sets database connection parameters read in from config file
    - Create cursor and connection objects
    - Set search path to music schema
    - Loads songs and event data json files into staging tables
    - Insert data into dimension tables and fact table from the staging tables
    
    Args: None
    Returns: None
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    dbname    = config.get("CLUSTER","dbname")
    user      = config.get("CLUSTER","user")
    password  = config.get("CLUSTER","password")
    port      = config.get("CLUSTER","port")
    
    # copy host name from AWS redshift dashboard
    DWH_ENDPOINT='dwhcluster.cyihktxlk33b.us-west-2.redshift.amazonaws.com'

    connection_str = "host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, dbname, user, password, port)
    conn = psycopg2.connect(connection_str)
    
    cur = conn.cursor()
    
    define_search_path(cur, conn)
    
    print("loading staging tables..")
    load_staging_tables(cur, conn)
    
    print("insert tables..")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()