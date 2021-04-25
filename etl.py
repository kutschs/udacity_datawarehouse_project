import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def execute_query_list(cur, conn, query_list):
    """
    Execute the queries in query_list using curser cur on connection conn.
    """
    try:
        for query in query_list:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print("Error executing query list")
        print(e)


def main():
    """
    - Establishes connection with the sparkify database and gets
      cursor to it.
    
    - copy data from S3 to staging tables in redshift
    - fill analytical tables from staging tables
    
    - Finally, closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    try: 
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    
        # copy data from S3 to staging tables in redshift
        print('Staging song and log data. This takes several minutes!')
        execute_query_list(cur, conn, copy_table_queries)
        
        print('Populating analytics tables.')
        # fill analytical tables from staging tables
        execute_query_list(cur, conn, insert_table_queries)
    finally:
        conn.close()


if __name__ == "__main__":
    main()