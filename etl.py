import configparser
import psycopg2 # type: ignore
import logging
from sql_queries import copy_table_queries, insert_table_queries # type: ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
    handlers=[
        logging.StreamHandler()  # You can add FileHandler to log to a file
    ]
)

def load_staging_tables(cur, conn):
    """
    Loads data from S3 into the staging tables on Redshift.
    """
    for query in copy_table_queries:
        try:
            logging.info(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()
            logging.info("Query executed successfully")
        except Exception as e:
            logging.error(f"Error executing query: {query}")
            logging.error(e)

def insert_tables(cur, conn):
    """
    Inserts data from the staging tables into the analytics tables on Redshift.
    """
    for query in insert_table_queries:
        try:
            logging.info(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()
            logging.info("Query executed successfully")
        except Exception as e:
            logging.error(f"Error executing query: {query}")
            logging.error(e)

def validate_counts(cur):
    """
    Validates the ETL process by counting the records in each table.
    """
    tables = [
        "staging_events",
        "staging_songs",
        "songplays",
        "users",
        "songs",
        "artists",
        "time"
    ]
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        results = cur.fetchone()
        logging.info(f"SELECT COUNT(*) FROM {table}: {results[0]}")

def main():
    """
    - Reads the configuration file to get the Redshift cluster details.
    - Establishes a connection to the Redshift cluster.
    - Loads data into staging tables.
    - Inserts data into the analytics tables.
    - Validates the ETL process.
    - Closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn_string="postgresql://{}:{}@{}:{}/{}".format(config['DWH']['DWH_DB_USER'], config['DWH']['DWH_DB_PASSWORD'], config['DWH']['DWH_ENDPOINT'], config['DWH']['DWH_PORT'],config['DWH']['DWH_DB'])

    try:
        logging.info("Connecting to Redshift")
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        logging.info("Connection established")

        logging.info("Loading data into staging tables")
        load_staging_tables(cur, conn)
        logging.info("Data loaded into staging tables")

        logging.info("Inserting data into analytics tables")
        insert_tables(cur, conn)
        logging.info("Data inserted into analytics tables")

        logging.info("Validating data counts")
        validate_counts(cur)

    except Exception as e:
        logging.error("Error in ETL process")
        logging.error(e)
    finally:
        conn.close()
        logging.info("Connection closed")

if __name__ == "__main__":
    main()