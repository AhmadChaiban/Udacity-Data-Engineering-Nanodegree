import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3


def drop_tables(cur, conn):
    """
        This function drops the tables if 
        they had been created before
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        This function creates the tables needed 
        for staging and creating the star schema
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The function that will get the necessary credentials from the 
    config file, retrieve the redshift cluster.
    
    The tables will then be dropped (if they exist)
    and created. 
    
    """
    
    ## Reading and getting necessary credentials
    config = configparser.ConfigParser()
    config.read('./RedshiftCluster/dwh.cfg')
    
    KEY                         = config.get('AWS','KEY')
    SECRET                      = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER      = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                      = config.get("DWH", "DWH_DB")
    DWH_USER                    = config.get("DWH", "DWH_DB_USER")
    DWH_PASSWORD                = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT                    = config.get("DWH", "DWH_PORT")

    
    ## retriving the cluster
    redshift = boto3.client('redshift',
                               region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                               )
    
    ## Getting the cluster properties and endpoint
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    
    ## Connecting to the database
    conn = psycopg2.connect(f"host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_USER} password={DWH_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()
    
    ## Dropping the tables
    drop_tables(cur, conn)
    ## Creating the tables
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()