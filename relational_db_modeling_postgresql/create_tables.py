import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Drop and Create sparkifydb.
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                            user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print ("Error: Dropping Database")
        print (e)

    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING \
                    'utf8' TEMPLATE template0;")
    except psycopg2.Error as e:
        print ("Error: Creating Database")
        print (e)
    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                            user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drop all tables in the studentdb
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Dropping table")
            print (e)


def create_tables(cur, conn):
    """
    Create all tables in the studentdb
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Creating table")
            print (e)


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
