import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):



"""
    Queries to clean the database by dropping if tables exists
    :param cur: cursor object to execute queries
    :param conn: pyscopg2 connection object to Redshift cluster
    :return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    DDL queries to be run to create staging tables, as well as fact and dimension tables
    :param cur: cursor object to execute queries
    :param conn: pyscopg2 connection object to Redshift cluster
    :return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Orchestrator function responsible to call drop and create table functions
    :return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()