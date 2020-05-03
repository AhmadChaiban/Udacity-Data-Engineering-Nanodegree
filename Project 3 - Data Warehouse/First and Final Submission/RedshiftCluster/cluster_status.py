import boto3
import configparser

## Getting the credentials and checking the cluster status
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                         = config.get('AWS','KEY')
SECRET                      = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER      = config.get("DWH","DWH_CLUSTER_IDENTIFIER")


redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(myClusterProps['ClusterStatus'])