import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging tables
    """
    for idx, query in enumerate(copy_table_queries):
        try:
            cur.execute(query)
            conn.commit()
            print("Success: Loading Table {}".format(idx))
        except psycopg2.Error as e:
            print("Error: Loading Table {}".format(idx))
            print (e)


def insert_tables(cur, conn):
    """
    Create cube tabls and save these as new tables
    """
    for idx, query in enumerate(insert_table_queries):
        try:
            cur.execute(query)
            conn.commit()
            print("Success: Inserting Table {}".format(idx))
        except psycopg2.Error as e:
            print("Error: Inserting into table {}".format(idx))
            print (e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} \
                            password={} port={}".format(
                            *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
