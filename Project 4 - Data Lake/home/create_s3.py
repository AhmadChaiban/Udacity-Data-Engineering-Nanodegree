import logging
import boto3
from botocore.exceptions import ClientError
## The 7bb globlin
from glob import glob as globlin
import configparser


def get_key_secret():
    
    """
        Getting key and secret from the config fiels
        
        :return: AWS Access key id, 
                 AWS secrety access key
    """
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    KEY = config['AWS']['AWS_ACCESS_KEY_ID']
    SECRET = config['AWS']['AWS_SECRET_ACCESS_KEY']
    return KEY, SECRET


def get_data_files(main_directory):
    """
        Using glob to get the paths of all the file names
        to be uploaded
        
        :param main_directory: the main directory to begin searching
        :return: list of log file paths, 
                 list of song file paths
        
    """
    print('************************************')
    print('Log data list')
    print('************************************')
    log_files_list = globlin(main_directory + '/*/*.json' , recursive=True)
    song_files_list = globlin(main_directory + '/*/*/*/*/*.json', recursive=True)
    print(log_files_list)
    print('************************************')
    print('Song data list')
    print('************************************')
    print(song_files_list)
    return log_files_list, song_files_list


def create_bucket(bucket_name, KEY, SECRET, region=None):
    """
        Create an S3 bucket in us-west-2

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :param KEY: The AWS access key
        :param SECRET: AWS secret access key
        :return: s3_client upon creation, exit otherwise
    """
    ## Creating the bucket 
    try:
        if region is None:
            s3_client = boto3.client('s3', 
                                     aws_access_key_id=KEY,
                                     aws_secret_access_key=SECRET)
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', 
                                     region_name=region,
                                     aws_access_key_id=KEY,
                                     aws_secret_access_key=SECRET)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        print('Could not create')
        exit()
        
    print('************************************')
    print('Create S3 Client')
    print('************************************')
    return s3_client
       
        
        
def upload_file_s3(file_name, bucket):
    """
        Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: s3 Bucket name to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, 
                 else False
    """

    # If S3 object_name was not specified, use file_name    
    try:
        response = s3_client.upload_file(file_name.replace('./',''), 
                                         bucket, 
                                         file_name.replace('./',''))
        print("Uploaded " + file_name)
    except ClientError as e:
        print("Failed to upload " + file_name)
        logging.error(e)
        return False
    return True



def upload_files_s3(files, bucket):
    """
        uploading multiple files to the s3 instance
        
        :param files: a list of files to upload
        :bucket: name of the s3 bucket
    """
        
    print('************************************')
    print('Uploading files to s3 bucket...')
    print('************************************')
    
    for i in range(len(files)):
        upload_file_s3(files[i], bucket)
    
    print('************************************')
    print('Upload complete')
    print('************************************')
    
        
        
if __name__ == '__main__':
    
    KEY, SECRET = get_key_secret()
    
    s3_client = create_bucket('udacity-data-lake-project-2187',
                              KEY,
                              SECRET,
                              'us-west-2')
    
    log_files_list, song_files_list = get_data_files('./ExtractedData')
    
    upload_files_s3(log_files_list, 
                    'udacity-data-lake-project-2187')
    
    upload_files_s3(song_files_list, 
                    'udacity-data-lake-project-2187')
    
    
    
    