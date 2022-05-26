#!/usr/bin/env python
# title           :S3Connections.py
# description     :This will create a header for a python script.
# author          :Darwin Uy
# date            :2021-1-11
# version         :0.2
# usage           :python pyscript.py
# notes           :
# python_version  :3.9
# ==============================================================================
import boto3


def createS3Client(s3Key, s3Secret):
    ''' This function creates a S3 Client
        Input:
            s3Key: AWS S3 access key
            s3Secret: AWS S3 secret access key
        Output:
            s3client: A s3 client
        '''
    s3client = boto3.client('s3', aws_access_key_id=s3Key, aws_secret_access_key=s3Secret)
    return s3client


def createS3Session(s3AccessKey, s3Secret):
    ''' This function creates a S3 Session
        Input:
            s3Key: AWS S3 access key
            s3Secret: AWS S3 secret access key
        Output:
            s3Session: A S3 session
        '''
    s3Session = boto3.Session(aws_access_key_id=s3AccessKey,
                              aws_secret_access_key=s3Secret)
    return s3Session


def createS3Resource(s3Session):
    ''' Description: This is a function that creates a S3 Resource from a given S3 session
        Input:
            session: a S3 Session
                - output from createS3Session(s3Key, s3Secret)
        Output:
            A S3 Resource
        '''
    s3Resource = s3Session.resource('s3')
    return s3Resource


def s3Gets3Items(s3Client, s3Bucket, s3Folder):
    ''' Description: This function gets all items in a S3 folder
        Input:
            s3Client: A S3 client instance
            Bucket: A S3 bucket
            Prefix: S3 "Folder" where we want to retrieve all the items
                - includes both folders and objects
        Output:
            A list of all items inside the bucket prefix.
                - includes folders and files
     '''
    items = s3Client.list_objects(Bucket=s3Bucket, Prefix=s3Folder)
    return items


def s3GetInputFolder(FolderItems):
    ''' Description: This is a function that That finds all the input folders that are in a list of S3 objects,
            - items end with "INPUT/"
        Input:
            FolderItems: List of S3 items from a selected keyZ
        Output:
            List of Input folders in a S3 prefix
        '''
    inputFolderlist = []
    for item in FolderItems['Contents']:
        if str(item['Key']).endswith('input/'):
            inputFolderlist.append(item['Key'])
    return (inputFolderlist)


def s3GetInputFiles(s3Client, inputFolderlist, s3Bucket):
    ''' Description: This is a function that finds all object file keys located in the input folders.
        Input:
            s3Client: A S3 client instance
            inputFolderlist: List of folders to find input files in
            s3Bucket: A S3 bucket
        Output:
            List of files to be imported
        '''
    file_list = []
    for folder in inputFolderlist:
        items = s3Client.list_objects(Bucket=s3Bucket, Prefix=str(folder))  # get items from input folder
        for item in items['Contents']:  # get the file keys to be input
            if item['Key'][-1:] != '/':
                file_list.append(item['Key'])
    return file_list


def s3GetObject(s3Client, s3Bucket, s3Key):
    ''' Description: This is a function that gets and object from a given S3 location
        Input:
            s3Client: A S3 client instance
            s3Bucket: A S3 bucket
            s3Key: AWS S3 access key
        Output:
        '''
    object = s3Client.get_object(Bucket=s3Bucket, Key=s3Key)
    return object


def s3Copy(s3Resource, s3DestinationBucket, s3DestinationKey, s3SourceBucket, s3SourceKey):
    ''' Description: This is a function that
        Input:
            s3Resource: a S3 resource instance
            s3DestinationBucket: S3 Destination Bucket
            s3DestinationKey: S3 Destination Key for object
            s3SourceBucket: S3 Source Bucket
            s3SourceKey: S3 Source Key for object
        Result:

        '''
    s3Resource.Object(s3DestinationBucket, s3DestinationKey).copy_from(
        CopySource={'Bucket': s3SourceBucket, 'Key': s3SourceKey})
    print('S3 object Copied to location')


def s3Delete(s3Resource, s3Bucket, s3Key):
    ''' Description: This is a function that
        Input:
            s3Bucket: A S3 bucket
            s3Key: AWS S3 access key
        Result:
            S3 object gets deleted
        '''
    s3Resource.Object(s3Bucket, s3Key).delete()
    print('S3 object deleted')


def s3Move(s3Resource, s3DestinationBucket, s3DestinationKey, s3SourceBucket, s3SourceKey):
    ''' Description: This is a function that
        Input:
            s3Resource: A S3 Resource instance
            s3DestinationBucket: S3 Destination Bucket
            s3DestinationKey: S3 Destination object key
            s3SourceBucket: S3 Source Bucket
            s3SourceKey: S3 Source object key
        Result:
            S3 Object moved
        '''
    s3Copy(s3Resource, s3DestinationBucket, s3DestinationKey, s3SourceBucket, s3SourceKey)
    s3Delete(s3Resource, s3SourceBucket, s3SourceKey)
    print('S3 Object moved')
