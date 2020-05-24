import boto3
from botocore.exceptions import ClientError
import time
import json
from config_details import get_cluster_details


def user_feedback(message):
    print('***************************************')
    print(message)
    print('***************************************')


def check_status_get_endpoint_properties(DWH_CLUSTER_IDENTIFIER):
    """
    Checking the status of the cluster and returning
    the required information when the cluster becomes
    available.

    :param DWH_CLUSTER_IDENTIFIER:
    :return: DWH_ENDPOINT, The redshift cluster endpoint
             DWH_ROLE_ARN, the iam role arn
             myClusterProps, the rest of the redshift cluster properties
    """
    while True:
        time.sleep(20)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = myClusterProps['ClusterStatus']
        user_feedback(f"Current Cluster Status: {cluster_status}")
        if cluster_status.lower() == 'available':
            DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
            DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
            return DWH_ENDPOINT, DWH_ROLE_ARN, myClusterProps


def create_clients(KEY, SECRET):
    """
        Creates the necessary recources and clients
        that will be used to create the redshift cluster

        :return: ec2, iam, redshift clients and resources
    """
    ec2 = boto3.resource(
        'ec2',
         region_name="us-west-2",
         aws_access_key_id=KEY,
         aws_secret_access_key=SECRET
    )

    iam = boto3.client(
        'iam',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name='us-west-2'
    )

    redshift = boto3.client(
        'redshift',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    user_feedback('Clients Received.')

    return ec2, iam, redshift


def get_iam_role(DWH_IAM_ROLE_NAME, iam):
    """
        Creating the iam role for access

        :return: the IAM role ARN
    """
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    user_feedback('IAM received.')
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

        :param: various cluster properties required.
        :return: DWH_ENDPOINT, The redshift cluster endpoint
                 DWH_ROLE_ARN, the iam role arn
                 myClusterProps, the rest of the redshift cluster properties
    """
    try:
        user_feedback('Initializing Cluster')
        response = redshift.create_cluster(
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName = DWH_DB,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles = [roleArn]
        )
    except Exception as e:
        print(e)

    DWH_ENDPOINT, DWH_ROLE_ARN, myClusterProps = check_status_get_endpoint_properties(DWH_CLUSTER_IDENTIFIER)
    user_feedback(f'Cluster Created. Use the following Endpoint with Airflow: {DWH_ENDPOINT}')

    return DWH_ENDPOINT, DWH_ROLE_ARN, myClusterProps


def vpc_endpoint(myClusterProps, DWH_PORT, ec2):
    """
    Configuring the cluster vpc

    :param myClusterProps: The redshift cluster properties
    :param DWH_PORT: the warehouse port
    :param ec2: the ec2 resource
    :return: None, just configures the cluster
    """
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName = defaultSg.group_name,
            CidrIp = '0.0.0.0/0',
            IpProtocol = 'TCP',
            FromPort = int(DWH_PORT),
            ToPort = int(DWH_PORT)
        )
    except Exception as e:
        print(e)



if __name__=='__main__':

    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME = get_cluster_details()


    ec2, iam, redshift = create_clients(KEY, SECRET)

    roleArn = get_iam_role(DWH_IAM_ROLE_NAME, iam)

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





