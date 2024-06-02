import configparser
import psycopg2 # type: ignore
import logging
import pandas as pd # type: ignore
from sql_queries import create_table_queries, drop_table_queries # type: ignore

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            logger.info(f"Dropping table with query: {query}")
            cur.execute(query)
            conn.commit()
            logger.info("Table dropped successfully")
        except Exception as e:
            logger.error(f"Error dropping table: {e}")
            conn.rollback()

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        try:
            logger.info(f"Creating table with query: {query}")
            cur.execute(query)
            conn.commit()
            logger.info("Table created successfully")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            conn.rollback()

def read_dwh_config(file_path):
    """
    Reads the configuration file and returns a DataFrame with configuration parameters.
    """
    config = configparser.ConfigParser()
    config.read_file(open(file_path))

    data = {
        "Param": ["KEY", "SECRET", "DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME", "DWH_ENDPOINT"],
        "Value": [
            config.get('AWS', 'KEY'),
            config.get('AWS', 'SECRET'),
            config.get("DWH", "DWH_CLUSTER_TYPE"),
            config.get("DWH", "DWH_NUM_NODES"),
            config.get("DWH", "DWH_NODE_TYPE"),
            config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
            config.get("DWH", "DWH_DB"),
            config.get("DWH", "DWH_DB_USER"),
            config.get("DWH", "DWH_DB_PASSWORD"),
            config.get("DWH", "DWH_PORT"),
            config.get("DWH", "DWH_IAM_ROLE_NAME"),
            config.get("DWH", "DWH_ENDPOINT")
        ]
    }

    return pd.DataFrame(data)

def get_config_value(df, param_name):
    """
    Retrieves a configuration value from the DataFrame given the parameter name.
    """
    value = df[df['Param'] == param_name]['Value']
    if not value.empty:
        return value.values[0]
    else:
        raise ValueError(f"Parameter {param_name} not found in configuration.")

def main():
    """
    - Reads the configuration file to get the Redshift cluster details.
    - Establishes a connection to the Redshift cluster.
    - Drops all the existing tables.
    - Creates all the tables needed for the analytics.
    - Closes the connection.
    """
    config_file_path = 'dwh.cfg'
    df = read_dwh_config(config_file_path)
    
    # Extract data from the configuration DataFrame
    try:
        KEY                     = get_config_value(df, 'KEY')
        SECRET                  = get_config_value(df, 'SECRET')
        DWH_IAM_ROLE_NAME       = get_config_value(df, 'DWH_IAM_ROLE_NAME')
        DWH_CLUSTER_TYPE        = get_config_value(df, 'DWH_CLUSTER_TYPE')
        DWH_NUM_NODES           = get_config_value(df, 'DWH_NUM_NODES')
        DWH_NODE_TYPE           = get_config_value(df, 'DWH_NODE_TYPE')
        DWH_CLUSTER_IDENTIFIER  = get_config_value(df, 'DWH_CLUSTER_IDENTIFIER')
        DWH_DB                  = get_config_value(df, 'DWH_DB')
        DWH_DB_USER             = get_config_value(df, 'DWH_DB_USER')
        DWH_DB_PASSWORD         = get_config_value(df, 'DWH_DB_PASSWORD')
        DWH_PORT                = get_config_value(df, 'DWH_PORT')
        DWH_ENDPOINT            = get_config_value(df, 'DWH_ENDPOINT')
    except ValueError as e:
        logger.error(e)
        exit(1)

    # Create the connection string
    conn_string = f"postgresql://{DWH_DB_USER}:{DWH_DB_PASSWORD}@{DWH_ENDPOINT}:{DWH_PORT}/{DWH_DB}"

    try:
        logger.info("Connecting to Redshift")
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        logger.info("Connection established")

        logger.info("Dropping tables")
        drop_tables(cur, conn)
        logger.info("Creating tables")
        create_tables(cur, conn)
    except Exception as e:
        logger.error(f"Error in main process: {e}")
    finally:
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()