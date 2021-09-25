import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_schema(cur, conn):
    """
    Create Redshift database schema called 'music'. Set search path to 'music' schema
    
    Args:
    cur: cursor object for executing database query
    conn: database connection object
    
    Returns: None
    """
    
    name = "music"
    query = "CREATE SCHEMA IF NOT EXISTS {};SET search_path TO {};".format(name,name)
    cur.execute(query)
    conn.commit()

    
def drop_tables(cur, conn):
    """
    Drop database tables
    
    Args:
    cur: cursor object for executing database query
    conn: database connection object
    
    Returns: None    
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create database tables
    
    Args:
    cur: cursor object for executing database query
    conn: database connection object
    
    Returns: None
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Called from command line as: python create_tables.py
    
    - Sets database connection parameters read in from config file
    - Create cursor and connection objects
    - Call method to create database schema and set search path
    - Call method to drop tables if they exist
    - Call method to create tables in database
    
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

    create_schema(cur, conn)
    
    print('drop tables..')
    drop_tables(cur, conn)
    
    print('create tables..')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()