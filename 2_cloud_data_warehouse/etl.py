import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This function is for load staging table from s3 
    The query from "copy_table_queries"
    '''
    for query in copy_table_queries:
#         print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    This function is for insert data from s3 to database
    The query from "insert_table_queries"
    '''
    for query in insert_table_queries:
#         print(query)
        cur.execute(query)
        conn.commit()


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