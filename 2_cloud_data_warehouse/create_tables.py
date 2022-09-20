import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    This function drop the tables in aws s3 by queries in "drop_table_queries"
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    This function create the tables in aws s3 by queries in "create_table_queries"
    '''
    for query in create_table_queries:
#         print(query)
        cur.execute(query)
        conn.commit()


def main():
    '''
    This function is for setup database on aws
    Step: 
        - drop the tables if it exist
        - create the new tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()