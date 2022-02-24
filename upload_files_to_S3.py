import configparser
import os
import boto3


config = configparser.ConfigParser()
config.read('aws_credentials.cfg')

KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
SECRET=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def upload_files(storage, filename_path, bucket_name, filename):
    """
    Upload the files to s3 bucket.
    
    Parameters:
        storage - storage.
        filename - file name and or including the path.
        bucket_name - s3 bucket name.
    
    Returns:
        N/A.
    """
    storage.meta.client.upload_file(Filename=filename_path, Bucket=bucket_name, Key=filename)
    

def retrieve_immigrants_files(storage, bucket_name):
    """
    - Pull immigrants files and then load the files to s3 bucket.
    
    Parameters:
        storage - storage.
        .bucket_name - s3 bucket name.
    
    Returns:
        N/A.
    """
    path = '/data/18-83510-I94-Data-2016'
    files = os.listdir(path)
    
    for file in files:
        full_path = path+'/'+file
        upload_files(storage, full_path, bucket_name, file)
    
    

def main():
    """
    - Create an session to access s3. 
    - Upload files to S3  
    """
    session = boto3.Session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    
    s3 = session.resource('s3')
    
    filenames = ['airport-codes_csv.csv', 'I94_SAS_Labels_Descriptions.SAS']
    bucket_name = 'udac-capstone-bucket'
    
    for filename in filenames:
        upload_files(s3, filename, bucket_name, filename)
        
    retrieve_immigrants_files(s3, bucket_name)

if __name__ == "__main__":
    main()