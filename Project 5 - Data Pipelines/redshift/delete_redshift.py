import boto3
import configparser
from create_redshift import user_feedback

## Getting the cluster in order to delete it and detach
## the iam role
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                         = config.get('AWS','KEY')
SECRET                      = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER      = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

user_feedback(f'Getting redshift cluster {DWH_CLUSTER_IDENTIFIER}')

redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

user_feedback(f'Deleted redshift cluster {DWH_CLUSTER_IDENTIFIER}')



# DWH_IAM_ROLE_NAME           = config.get("DWH", "DWH_IAM_ROLE_NAME")
# iam = boto3.client('iam',
#                    aws_access_key_id=KEY,
#                    aws_secret_access_key=SECRET,
#                    region_name='us-west-2'
#                    )

# iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
# iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)