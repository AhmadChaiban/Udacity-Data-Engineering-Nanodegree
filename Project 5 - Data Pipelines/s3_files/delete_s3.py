import boto3    
from create_s3 import get_key_secret

def get_s3(region, KEY, SECRET):
    """
        Getting the created s3, both client and resource
        
        :param region: the established s3's region
        :param KEY: AWS Access key id
        :param SECRET: AWS SECRET ACCESS KEY
        :return s3: returns the s3 resource and client
    """
    s3_resource = boto3.resource('s3', 
                                 region_name=region,
                                 aws_access_key_id=KEY,
                                 aws_secret_access_key=SECRET)
    
    s3_client = boto3.client('s3', 
                             region_name=region,
                             aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET)
    
    return s3_resource, s3_client

def delete_all(s3_resource, s3_client, bucket_name):
    """
        Emptying and Deleting the bucket
        
        :param s3_resource: The s3 resource (required to empty bucket)
        :param s3_client: the s3 client (required to delete bucket)
        :param bucket_name: Name of the s3 bucket to be deleted
    """
    bucket = s3_resource.Bucket(bucket_name)
    bucket.objects.all().delete()
    print('****************************')
    print('Emptied Bucket ' + bucket_name)
    print('****************************')
    s3_client.delete_bucket(Bucket = bucket_name)
    print('****************************')
    print('Deleted Bucket ' + bucket_name)
    print('****************************')

if __name__ == '__main__':
    KEY, SECRET = get_key_secret()
    s3_resource, s3_client = get_s3('us-west-2', KEY, SECRET)
    delete_all(s3_resource, s3_client, 'udacity-data-pipeline-project-2187')
    