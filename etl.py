import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    First step of ETL copying data from files in S3 bucket to staging tables in Redshigt
    :param cur: cursor object to execute queries
    :param conn: pyscopg2 connection object to Redshift cluster
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables in Redshift to modelled fact and dimension tables within the same Redshift cluster
    :param cur: cursor object to execute queries
    :param conn: pyscopg2 connection object to Redshift cluster
    :return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Orchestrator function to call function load data to staging and then into modelled tables
    :return: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()