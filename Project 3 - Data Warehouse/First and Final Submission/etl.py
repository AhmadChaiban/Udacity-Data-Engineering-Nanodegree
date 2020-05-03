import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import boto3

def load_staging_tables(cur, conn):
    """
        Copy to the tables from s3
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
            
        
def insert_tables(cur, conn):
    """
        Insert from the copied tables to 
        the star schema
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    
    """
        1. Get the necessary credentials from the config file 
        2. Retrieve the redshift cluster
        3. Connect to the database
        4. copy and insert into the tables
    """
    
    ## Getting the necessary credentials from the config
    config = configparser.ConfigParser()
    config.read('./RedshiftCluster/dwh.cfg')
    
    KEY                         = config.get('AWS','KEY')
    SECRET                      = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER      = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                      = config.get("DWH", "DWH_DB")
    DWH_USER                    = config.get("DWH", "DWH_DB_USER")
    DWH_PASSWORD                = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT                    = config.get("DWH", "DWH_PORT")

    ## Retrieving the cluster
    redshift = boto3.client('redshift',
                               region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                               )
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    
    ## Connecting to the database
    conn = psycopg2.connect(f"host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_USER} password={DWH_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()
    
    ## Copying and inserting into the tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()