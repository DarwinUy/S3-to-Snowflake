import yaml
import time

import S3Connection
import PandasProcessing
import SnowflakeConnection
import Communication

# Get configurations from the YAML file

with open(r'/home/ec2-user/production/S3_to_Snowflake/config/S3_Configurations.yaml') as file:
    s3Config = yaml.load(file, Loader=yaml.FullLoader)

with open(r'/home/ec2-user/production/S3_to_Snowflake/config/Snowflake_Configurations.yaml') as file:
    snowflakeConfig = yaml.load(file, Loader=yaml.FullLoader)


def splitFileName(fileKey):
    ''' Description: This is a function that parses the key to get the the database and table name for Schema
        Input:
            fileKey: this is the S3 key for the file

        Output:

        '''
    fileName = fileKey.split('/')[-1]
    sfDatabase = fileKey.split('/')[-3]
    sfTable = fileName.split('.')[0]
    return sfDatabase, sfTable, fileName


# Get Credentials
# S3
s3Bucket = s3Config['s3.bucket']
s3Key = s3Config['s3.key']
s3Secret = s3Config['s3.secret']
s3Folder = s3Config['s3.folder']
s3InternationalInput = s3Config['s3.internationalInputFolder']

# Snowflake
SfSchema = snowflakeConfig['sf.schema']
SfPassphrase = snowflakeConfig['sf.passphrase']
SfKeyfile = snowflakeConfig['p8.key.file']
sfUser = snowflakeConfig['sf.user']
sfAccount = snowflakeConfig['sf.account']
sfWarehouse = snowflakeConfig['sf.warehouse']
sfPrivateKey = SnowflakeConnection.getPrivateKey(keyFile=SfKeyfile, snowflakePassword=SfPassphrase)
sfRole = 'SYS_SOURCE'



def main():

    '''
    1) Create an S3 Client
    2) Get all items in a specified S3 folder
    3) Filter these items so we only get folder that contains files that need to be input
    4) list all the keys that need to be input into snowflake
    5) for each of these items
        a) Get the S3 object linking to the S3 key of interest
        b) Create a pandas dataframe
            - currently delimitting by file type
        c) Infer the schema for the dataframe created
        d) create the schema definition to import into snowflake
        e) Create the snowflake table
        f) Write to snowflake table
        g) Move files in S3
        h) Send out the emails

    :return:
    '''

    client = S3Connection.createS3Client(s3Key=s3Key, s3Secret=s3Secret)
    session = S3Connection.createS3Session(s3AccessKey=s3Key, s3Secret=s3Secret)
    resource = S3Connection.createS3Resource(s3Session=session)

    # moves files into file2table/team_international/input/ for international team
    InternationalInput = [s3InternationalInput]
    internationalInputFiles = S3Connection.s3GetInputFiles(s3Client = client, inputFolderlist = InternationalInput, s3Bucket = s3Bucket)
    if internationalInputFiles:
        for internationalFileKey in internationalInputFiles:
            _, _, fileName = splitFileName(fileKey=internationalFileKey)
            Destination = f"file2table/team_international/input/{fileName}"
            S3Connection.s3Move(s3Resource=resource, s3DestinationBucket=s3Bucket, s3DestinationKey=Destination,
                                s3SourceBucket=s3Bucket, s3SourceKey=internationalFileKey)
        time.sleep(30)


    items = S3Connection.s3Gets3Items(s3Client=client, s3Bucket=s3Bucket, s3Folder=s3Folder)
    inputFolders = S3Connection.s3GetInputFolder(FolderItems=items)
    inputFiles = S3Connection.s3GetInputFiles(s3Client=client, inputFolderlist=inputFolders, s3Bucket=s3Bucket)

    if not inputFiles:
        print ("No files to import")
        return

    for file in inputFiles:

        print(file)
        s3Object = S3Connection.s3GetObject(s3Client=client, s3Bucket=s3Bucket, s3Key=file)
        if file.endswith('.txt'):
            s3FileDF_0 = PandasProcessing.importToPandas(object=s3Object, delimiter='\t', header=True)
        elif file.endswith('.csv'):
            s3FileDF_0 = PandasProcessing.importToPandas(object=s3Object, delimiter='|', header=True)

        s3FileDF_1, _ = PandasProcessing.pandasInferSchema(s3FileDF_0)
        snowflakeSchemaDefinition_0 = PandasProcessing.getSchemaPandas2Snowflake(s3FileDF_1)
        sfDatabase, sfTable, sfFile = splitFileName(fileKey=file)


        try:
            snowflakeConnection = SnowflakeConnection.createSnowflakeConnection(sfAccount=sfAccount, sfUser=sfUser,
                                                                                sfPrivateKey=sfPrivateKey,
                                                                                sfWarehouse=sfWarehouse,
                                                                                sfDatabase=sfDatabase,
                                                                                sfSchema=SfSchema)
            SnowflakeConnection.createSnowflakeTable(sfConn=snowflakeConnection, sfRole=sfRole,
                                                     sfDatabase=sfDatabase,
                                                     sfSchema=SfSchema,
                                                     sfTable=sfTable, tableSchemaDef=snowflakeSchemaDefinition_0,
                                                     insert=False)
            success, nchunks, nrows = SnowflakeConnection.writePandas2Snowflake(sfConn=snowflakeConnection,
                                                                                pdDF=s3FileDF_1, sfTable=sfTable)
            # session = S3Connection.createS3Session(s3AccessKey=s3Key, s3Secret=s3Secret)
            # resource = S3Connection.createS3Resource(s3Session=session)
            destinationKey = f"file2table/{sfDatabase}/success_files/{sfFile}"
            sourceKey = file
            S3Connection.s3Move(s3Resource=resource, s3DestinationBucket=s3Bucket, s3DestinationKey=destinationKey,
                                s3SourceBucket=s3Bucket, s3SourceKey=sourceKey)

            subject = f"File uploaded to Snowflake from {sfDatabase}"
            message = f'Please be advised that {sourceKey} has been imported into snowflake as the table {sfDatabase}.{str(SfSchema).lower()}.{sfTable}. \n \n' \
                      f'Success is {success} with {nrows} rows loaded in {nchunks} chunks'
            Communication.send_mail(sender_email=sender_email, receiver_email=receiver_email, subject=subject,
                                    body=message, attachments=[])
        except Exception as err_message:
            # session = S3Connection.createS3Session(s3AccessKey=s3Key, s3Secret=s3Secret)
            # resource = S3Connection.createS3Resource(s3Session=session)
            destinationKey = f"file2table/{sfDatabase}/failed_files/{sfFile}"
            sourceKey = file
            S3Connection.s3Move(s3Resource=resource, s3DestinationBucket=s3Bucket, s3DestinationKey=destinationKey,
                                s3SourceBucket=s3Bucket, s3SourceKey=sourceKey)
            err_subject = f"{sfTable} Load Table Error"
            Communication.send_mail(sender_email=err_sender_email, receiver_email=err_receiver, subject=err_subject,
                                    body=str(err_message), attachments=[])


if __name__ == '__main__':
    main()
