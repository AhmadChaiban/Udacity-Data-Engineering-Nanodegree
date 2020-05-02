import configparser
import boto3
from botocore.exceptions import ClientError
import time
import json

def get_cluster_details():
    """
        Gets the credentials from the config file
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    return KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD,              DWH_PORT, DWH_IAM_ROLE_NAME


def create_clients(KEY, SECRET):
    """
        Creates the necessary recources and clients 
        that will be used to create the redshift cluster
    """
    ec2 = boto3.resource('ec2',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                       )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )

    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
    
    return ec2, s3, iam, redshift


def create_iam_role(DWH_IAM_ROLE_NAME, iam):
    """
        Creating the iam role for access
    """
    try:
        print("Creating a new IAM Role...") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
        print('done')
    except Exception as e:
        print(e)


    print("Attaching Policy...")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print('done')
    print("Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print('done')
    return roleArn
    
    
def create_redshift_cluster(DWH_CLUSTER_TYPE, 
                            DWH_NODE_TYPE, 
                            DWH_NUM_NODES,
                            DWH_DB,
                            DWH_CLUSTER_IDENTIFIER,
                            DWH_DB_USER,
                            DWH_DB_PASSWORD,
                            roleArn,
                            redshift):
    """
        Creating the redshift cluster. The function will stop running 
        only when the cluster becomes available
        Returning the endpoint, roleArn and other properties
    """
    try:
        response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)
        
    while True:
        time.sleep(20)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = myClusterProps['ClusterStatus']
        print(cluster_status)
        if cluster_status.lower() == 'available':
            DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
            DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']  
            return DWH_ENDPOINT, DWH_ROLE_ARN, myClusterProps


def vpc_endpoint(myClusterProps, DWH_PORT, ec2):
    """
        Setting up the vpc for the cluster
    """
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
        
        
def check_connection():
    """
        Veryfiying connectivity
    """
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
    print(conn_string)
    

if __name__=='__main__':
    
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT,     DWH_IAM_ROLE_NAME = get_cluster_details()
    
    
    ec2, s3, iam, redshift = create_clients(KEY, SECRET)
    
    roleArn = create_iam_role(DWH_IAM_ROLE_NAME, iam)
    
    DWH_ENDPOINT, DWH_ROLE_ARN, myClusterProps = create_redshift_cluster(DWH_CLUSTER_TYPE, 
                                                                         DWH_NODE_TYPE, 
                                                                         DWH_NUM_NODES,
                                                                         DWH_DB,
                                                                         DWH_CLUSTER_IDENTIFIER,
                                                                         DWH_DB_USER,
                                                                         DWH_DB_PASSWORD,
                                                                         roleArn,
                                                                         redshift)
    
    vpc_endpoint(myClusterProps, DWH_PORT, ec2)
    
    
        
    

        