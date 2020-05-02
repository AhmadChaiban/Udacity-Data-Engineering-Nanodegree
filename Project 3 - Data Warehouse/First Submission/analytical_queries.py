import configparser
import psycopg2
from sql_queries import row_checkers
import boto3

def check_number_of_rows(cur,conn):
    """
        This function runs the queries to get the 
        number of rows for each table that the data 
        has been inserted into
    """
    for query in row_checkers:
        cur.execute(query)
        row = cur.fetchone()
        print(row)
        conn.commit()
        
        
if __name__=='__main__':
    
    ## Reading from the config file

    config = configparser.ConfigParser()
    config.read('./RedshiftCluster/dwh.cfg')
    
    ## Getting the necessary credentials to retrieve the 
    ## redshift cluster
    
    KEY                         = config.get('AWS','KEY')
    SECRET                      = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER      = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                      = config.get("DWH", "DWH_DB")
    DWH_USER                    = config.get("DWH", "DWH_DB_USER")
    DWH_PASSWORD                = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT                    = config.get("DWH", "DWH_PORT")

    ## retrieving the created redshift cluster
    redshift = boto3.client('redshift',
                               region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                               )
    ## Getting the cluster properties and endpoint
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    
    ## Connecting to the redshift db
    conn = psycopg2.connect(f"host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_USER} password={DWH_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()
    
    ## Checking the number of rows per table
    check_number_of_rows(cur,conn)

    conn.commit()
    conn.close()