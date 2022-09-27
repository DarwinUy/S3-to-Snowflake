# title           :S3Connections.py
# description     :This will create a header for a python script.
# author          :Darwin Uy
# date            :2022-6-1
# version         :0.3
# usage           :python pyscript.py
# notes           :
# python_version  :3.9
# ==============================================================================
import boto3


def createS3Client(s3Key, s3Secret):
    """
    Description
    -----------
    a function that creates a S3 Client for accessing the Amazon S3 web service

    Args
    ----
    s3Key: string
        AWS S3 access key
    s3Secret: string
        AWS S3 secret access key

    Returns
    -------
    s3client: object
        A s3 client
    """
    s3client = boto3.client('s3', aws_access_key_id=s3Key, aws_secret_access_key=s3Secret)
    return s3client


def createS3Session(s3AccessKey, s3Secret):
    """
    Description
    -----------
    This function creates a S3 Session to store the configurations state

    Args
    ----
    s3Key: string
        AWS S3 access key
    s3Secret: string
        AWS S3 secret access key

    Returns
    -------
    s3Session: object
        A S3 session instance
    """
    s3Session = boto3.Session(aws_access_key_id=s3AccessKey,
                              aws_secret_access_key=s3Secret)
    return s3Session


def createS3Resource(s3Session):
    """
    Description
    -----------
    a function that creates a resource oriented interface for Amazon S3

    Args
    ----
    session: object
        a S3 Session
        - output from createS3Session(s3Key, s3Secret)

    Returns
    -------
    s3Resource: object
        S3 Resource instance
    """
    s3Resource = s3Session.resource('s3')
    return s3Resource


def s3Gets3Items(s3Client, s3Bucket, s3Folder):
    """
    Description
    -----------
    A function that gets a list of all items in a S3 folder

    Args
    ----
    s3Client: object
        A S3 client instance
    Bucket: string
        S3 bucket used
    s3Folder: string
        S3 path to folder of interest

    Returns
    -------
    items : list
        A list of all items inside the bucket prefix.
        - includes folders and files
    """
    objects = s3Client.list_objects(Bucket=s3Bucket, Prefix=s3Folder)
    items = [x['Key'] for x in objects['Contents']]
    return items


def s3GetInputFolder(FolderItems):
    """
    Description
    -----------
    This is a function that finds all the input folders that are in a list of S3 paths
    - items end with "INPUT/"

    Args
    ----
    FolderItems: list
        List of S3 paths

    Returns
    -------
    inputFolderlist: list
        List of Input folders in a S3 location
    """
    inputFolderlist = [item for item in FolderItems if item.endswith('input/')]
    return inputFolderlist


def s3GetInputFiles(s3Client, inputFolderlist, s3Bucket):
    """
    Description
    -----------
    This is a function that retrieves all object paths located in the input folders.

    Args
    ----
    s3Client: object
        A S3 client instance
    inputFolderlist: list
        List of folders to find input files in
    s3Bucket: string
        S3 bucket in use

    Returns
    -------
    file_list: list
        List of files to be in folder
    """
    inputFolderlist_objects = [s3Client.list_objects(Bucket=s3Bucket, Prefix=str(folder)) for folder in inputFolderlist]
    input_file_contents = [item["Contents"] for item in inputFolderlist_objects if "Contents" in item.keys()]
    input_folder_file_keys_list_unflattened = [[item["Key"] for item in i] for i in input_file_contents]
    input_folder_file_keys_list_flattened = [item for sublist in input_folder_file_keys_list_unflattened for item in
                                             sublist]
    input_files = [file for file in input_folder_file_keys_list_flattened if not file.endswith("/")]
    return input_files


def s3GetObject(s3Client, s3Bucket, s3Key):
    """
    Description
    -----------
    a function that gets an object from a given S3 location. Retrieves an object into memory as a raw vector.

    Args
    ----
    s3Client: object
        A S3 client instance
    s3Bucket: string
        A S3 bucket
    s3Key: string
        AWS S3 access key

    Returns
    -------
    object: object
        an S3 object
    """
    object = s3Client.get_object(Bucket=s3Bucket, Key=s3Key)
    return object


def s3Copy(s3Resource, s3DestinationBucket, s3DestinationKey, s3SourceBucket, s3SourceKey):
    """
    Description
    -----------
    A function that copies an object from one S3 location to another

    Args
    ----
    s3Resource: object
        a S3 resource instance
    s3DestinationBucket: string
        S3 Destination Bucket
    s3DestinationKey: string
        S3 Destination Key for object
    s3SourceBucket: string
        S3 Source Bucket
    s3SourceKey: string
        S3 Source Key for object

    Returns
    -------
    None
    """
    s3Resource.Object(s3DestinationBucket, s3DestinationKey).copy_from(
        CopySource={'Bucket': s3SourceBucket, 'Key': s3SourceKey})
    print('S3 object Copied to location')


def s3Delete(s3Resource, s3Bucket, s3Key):
    """
    Description
    -----------
    A function that deletes an object in S3

    Args
    ----
    s3Resource: object
        an S3 Resource instance
    s3Bucket: string
        A S3 bucket
    s3Key: string
        AWS S3 access key

    Returns
    -------
    None
    """
    s3Resource.Object(s3Bucket, s3Key).delete()
    print('S3 object deleted')


def s3Move(s3Resource, s3DestinationBucket, s3DestinationKey, s3SourceBucket, s3SourceKey):
    """
    Description
    -----------
    a function that moves an object from one S3 location to another

    Args
    ----
    s3Resource: object
        A S3 Resource instance
    s3DestinationBucket: string
        S3 Destination Bucket
    s3DestinationKey: string
        S3 Destination object path
    s3SourceBucket: string
        S3 Source Bucket
    s3SourceKey: string
        S3 Source object path

    Returns
    -------
    None
    """
    s3Copy(s3Resource, s3DestinationBucket, s3DestinationKey, s3SourceBucket, s3SourceKey)
    s3Delete(s3Resource, s3SourceBucket, s3SourceKey)
    print('S3 Object moved')
