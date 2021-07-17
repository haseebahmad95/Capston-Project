import configparser
import psycopg2
import psycopg2.extras
from sql_queries import ( 
    copy_table_queries, 
    insert_table_queries,
    data_cleansing_queries,
    data_quality_check_queries 
    )

def load_staging_tables(cur, conn):
    """
    Copy the netflix data from s3 to redshift
    Stagging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert the data from stagging tables 
    to dimension and fact tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def data_cleansing(cur, conn):
    """
    RUN data cleansing script
    """
    for query in data_cleansing_queries:
        cur.execute(query)
        conn.commit()

def run_quality_checks(cur, conn):
    """
    RUN data Quality checks
    """
    flg = 0
    for query in data_quality_check_queries:
        cur.execute(query)
        data = cur.fetchall()
        if(data[0][0] > 0):
            print(f'Data Quality Check failed Query: {query}')
            flg = 1

    if flg == 0:
        print('Data Quality Checks passed')




def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    data_cleansing(cur, conn)
    run_quality_checks(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()