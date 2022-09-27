# title					  : Main.py
# description			  : This is the main file for the S3_to_SF program
# author				  : Darwin Uy
# date					  : 2022/6/10
# version				  : 1.0
# usage					  : automated
# notes					  :
# Updates                 : 2022-6-10 added parsing function for file name and
#                                     error log added
#                                     more descriptive email messages
#                                     list generation instead of for loops
#                                     chunk loading
# python_version		  : 3.9
# _______________________________________________________________________________________________________________________
import yaml
import time
import csv
import pandas
import os

import S3Connection
import PandasProcessing
import SnowflakeConnection
import Communication


def splitFileName(fileKey):
    """
    Description
    -----------
    This is a function that parses the key to get the the database and table name for Schema

    Args
    ----
    fileKey : string
        this is the S3 file path for the file to have name parsed

    Returns
    -------
    sfDatabase : string
        string indicating Snowflake Database
    sfTable : string
        string indicating table name to use
        - extracted from file name
    fileName : string
        string indicating file name
        -includes .<type>
    """
    fileName = fileKey.split('/')[-1]
    sfDatabase = fileKey.split('/')[-3]
    sfTable = fileName.split('.')[0]
    return sfDatabase, sfTable, fileName


def fix_table_col_names(row):
    """
    Description
	-----------
	A function that reads a file and removes special characters from text and replaces them with a _
	    - for column names and table name
	    - removed characters: \'"!@#%^&*()-=+[]{}:;<>,.?/~`

    Args
	----
	row : string
	    text to have string replacement

    Returns
	-------
    clean_row : string
        string with problem characters replaced with underscore
    """
    invalid_char = """\\'"!@#%^&*()-=+[]{}:;<>,.?/~` """
    clean_row_list = ["_" if char in invalid_char else f"{char}" for char in row]
    clean_row = "".join(clean_row_list)
    return clean_row


def s3_to_sf(config_yaml_path):
    """
    Description
    -----------
    A function that writes file from an S3 input folder to a Snowflake table

    Args
    ----
    config_yaml_path : string
        path to yaml file specifying configurations to be used

    Returns
    -------
    None
    """
    # get configurations
    mismaches_in_a_row_limit = 50

    with open(config_yaml_path) as file:
        Config = yaml.load(file, Loader=yaml.FullLoader)

    common_config = Config['Common']
    s3_config = Config['AWS']
    snowflake_config = Config['Snowflake']
    email_config = Config['Email']

    # Get Credentials
    temp_folder = common_config["linux.temp_path"]

    # S3
    s3Bucket = s3_config['s3.bucket']
    s3Key = s3_config['s3.key']
    s3Secret = s3_config['s3.secret']
    s3Folder = s3_config['s3.folder']
    s3InternationalInput = s3_config['s3.internationalInputFolder']

    # Snowflake
    SfSchema = snowflake_config['sf.schema']
    SfPassphrase = snowflake_config['sf.passphrase']
    SfKeyfile = snowflake_config['p8.key.file']
    sfUser = snowflake_config['sf.user']
    sfAccount = snowflake_config['sf.account']
    sfWarehouse = snowflake_config['sf.warehouse']
    sfPrivateKey = SnowflakeConnection.getPrivateKey(keyFile=SfKeyfile, snowflakePassword=SfPassphrase)
    sfRole = snowflake_config['sf.Role']

    # Email
    sender_email = email_config['email.sender']
    err_sender_email = email_config['email.error_sender']

    # Create S3 Instances
    client = S3Connection.createS3Client(s3Key=s3Key, s3Secret=s3Secret)
    session = S3Connection.createS3Session(s3AccessKey=s3Key, s3Secret=s3Secret)
    resource = S3Connection.createS3Resource(s3Session=session)

    # moves files into file2table/team_international/input/ for international team
    internationalInputFiles = S3Connection.s3GetInputFiles(s3Client=client, inputFolderlist=[s3InternationalInput],
                                                           s3Bucket=s3Bucket)
    if internationalInputFiles:
        for internationalFileKey in internationalInputFiles:
            _, _, fileName = splitFileName(fileKey=internationalFileKey)
            Destination = f"file2table/team_international/input/{fileName}"
            S3Connection.s3Move(s3Resource=resource, s3DestinationBucket=s3Bucket, s3DestinationKey=Destination,
                                s3SourceBucket=s3Bucket, s3SourceKey=internationalFileKey)
        time.sleep(3)

    ## get input files
    items = S3Connection.s3Gets3Items(s3Client=client, s3Bucket=s3Bucket, s3Folder=s3Folder)
    inputFolders = S3Connection.s3GetInputFolder(FolderItems=items)
    inputFiles = S3Connection.s3GetInputFiles(s3Client=client, inputFolderlist=inputFolders, s3Bucket=s3Bucket)
    if not inputFiles:
        print("No files to import")
        return
    for file in inputFiles:
        start = time.time()
        print(f"{file} ingestion started")

        try:
            sfDatabase, sfTable, sfFile = splitFileName(fileKey=file)
            sfTable_name = fix_table_col_names(sfTable)

            # assign emails
            if sfDatabase.lower() == "team_A":
                receiver_email = [email_config['email. A'], email_config['email.B']]
            elif sfDatabase.lower() == "team_B":
                receiver_email = [email_config['email.B']]
            elif sfDatabase.lower() == "team_C":
                receiver_email = [email_config['email. C'], email_config['email.C']]
            else:
                receiver_email = [email_config['email.B']]

            ## assign delimeter
            if file.endswith('.txt'):
                delimiter = "|"
            elif file.endswith('.csv'):
                delimiter = "|"
            else:
                continue

            ## try to open the S3 file
            s3_object = S3Connection.s3GetObject(s3Client=client, s3Bucket=s3Bucket, s3Key=file)
            file_size = client.head_object(Bucket=s3Bucket, Key=file)['ContentLength']
            s3_object_body = s3_object['Body']

            # number of bytes to read per chunk
            mebibytes = 128
            chunk_size = (1024 ** 2) * mebibytes

            # the character that we'll split the data with (bytes, not string)
            header_chunk = True
            start_line_number = 2
            newline = '\n'.encode()
            partial_chunk = b''
            number_o_chunks = 0
            total_rows_loaded = 0

            text_file_errors = open(f"{temp_folder}/{sfTable}_errors.txt", "w")
            error_attatchment = []
            while (True):
                # If nothing was read there is nothing to process
                if chunk == b'':
                    break
                last_newline = chunk.rfind(newline)
                number_o_chunks += 1
                chunk = partial_chunk + s3_object_body.read(chunk_size)
                # write to a smaller file, or work against some piece of data
                s3_body_chunk = chunk[0:last_newline + 1].decode('utf-8').splitlines()
                if header_chunk == True:
                    table_header_row = s3_body_chunk[0].strip()
                    original_column_names = table_header_row.split(delimiter)
                    s3_body_chunk = s3_body_chunk[1:]
                    clean_table_header_row = fix_table_col_names(table_header_row)
                    clean_column_names = clean_table_header_row.split(delimiter)
                    clean_column_names = [f"N{column_name}" if column_name[0].isdigit() else f"{column_name}" for
                                          column_name in
                                          clean_column_names]
                    df = pandas.DataFrame(columns=clean_column_names)
                    column_count = len(clean_column_names)
                    column_name_changes = [f"{original_column_name} renamed to {column_name}\n" for
                                           (original_column_name, column_name) in
                                           list(zip(original_column_names, clean_column_names)) if
                                           (original_column_name != column_name)]
                    column_name_changes_string = "".join(column_name_changes)

                s3_body_chunk_split_rows = [row.strip().split(delimiter) for row in
                                            s3_body_chunk]  # get split rows to get list of lists
                endstart_line_number = start_line_number + len(s3_body_chunk_split_rows)
                line_number_list = list(range(start_line_number, endstart_line_number))
                row_col_count = [len(split_row) for split_row in s3_body_chunk_split_rows]  # count columns
                chunk_data = list(zip(line_number_list, row_col_count, s3_body_chunk_split_rows,
                                      s3_body_chunk))  # line number, number of columns in a row, split rows, original row

                ## get bad data
                errored_data = [[line_no, unsplit_row] for line_no, col_count, split_row, unsplit_row in chunk_data if
                                col_count != column_count]
                errored_data_string_list = [
                    f"line {line_number}: expected {column_count} columns but read {line.count(delimiter) + 1} [{line}]"
                    for
                    line_number, line in errored_data]
                errored_data_message = "\n".join(errored_data_string_list)
                text_file_errors.write(f"{errored_data_message}\n")
                if len(errored_data) > 0:
                    error_attatchment = [f"{temp_folder}/{sfTable}_errors.txt"]

                ## get data  and insert into pandas inserting
                loading_data = [split_row for line_no, col_count, split_row, unsplit_row in chunk_data if
                                col_count == column_count]
                append_row = pandas.DataFrame(loading_data, columns=clean_column_names)
                df = pandas.concat([df, append_row], axis=0)

                # create the snowflake table
                if header_chunk == True:
                    header_chunk = False
                    s3FileDF, _ = PandasProcessing.pandasInferSchema(df)
                    snowflakeSchemaDefinition = PandasProcessing.getSchemaPandas2Snowflake(s3FileDF)
                    snowflakeConnection = SnowflakeConnection.createSnowflakeConnection(sfAccount=sfAccount,
                                                                                        sfUser=sfUser,
                                                                                        sfPrivateKey=sfPrivateKey,
                                                                                        sfWarehouse=sfWarehouse,
                                                                                        sfDatabase=sfDatabase,
                                                                                        sfSchema=SfSchema)
                    create_sql = SnowflakeConnection.createSnowflakeTable(sfConn=snowflakeConnection, sfRole=sfRole,
                                                                          sfDatabase=sfDatabase, sfSchema=SfSchema,
                                                                          sfTable=sfTable_name,
                                                                          tableSchemaDef=snowflakeSchemaDefinition,
                                                                          insert=True)

                success, nchunks, nrows = SnowflakeConnection.writePandas2Snowflake(sfConn=snowflakeConnection,
                                                                                    pdDF=df, sfTable=sfTable_name)
                total_rows_loaded += nrows

                ## truncate
                df.drop(df.index, inplace=True)

                # keep the partial line you've read here
                partial_chunk = chunk[last_newline + 1:]
                start_line_number = endstart_line_number

            destinationKey = f"file2table/{sfDatabase}/success_files/{sfFile}"
            S3Connection.s3Move(s3Resource=resource, s3DestinationBucket=s3Bucket, s3DestinationKey=destinationKey,
                                s3SourceBucket=s3Bucket, s3SourceKey=file)

            text_file_errors.close()
            end = time.time()
            duration = end - start

            subject = f"File uploaded to Snowflake from {sfDatabase}"
            if column_name_changes_string:
                if len(error_attatchment) == 0:
                    message = f'Please be advised that {sfFile} has been imported into snowflake as the table {sfDatabase}.{str(SfSchema).lower()}.{sfTable_name}.\n\n' \
                              f'Success is {success} with {total_rows_loaded} rows loaded in {number_o_chunks} chunks\n\n' \
                              f'The following column names were converted:\n{column_name_changes_string}\n\n' \
                              f'file size: {file_size / (1024 ** 2)} mebibytes \n\ntime: {duration} seconds'
                else:
                    message = f'Please be advised that {sfFile} has been imported into snowflake as the table {sfDatabase}.{str(SfSchema).lower()}.{sfTable_name}.\n\n' \
                              f'Success is {success} with {total_rows_loaded} rows loaded in {number_o_chunks} chunks\n\n' \
                              f'The following column names were converted:\n{column_name_changes_string}\n\n' \
                              f'error log attached\n\n' \
                              f'file size: {file_size / (1024 ** 2)} mebibytes \n\ntime: {duration} seconds'
                del errored_data_message
            else:
                if len(error_attatchment) == 0:
                    message = f'Please be advised that {sfFile} has been imported into snowflake as the table {sfDatabase}.{str(SfSchema).lower()}.{sfTable_name}.\n\n' \
                              f'Success is {success} with {total_rows_loaded} rows loaded in {number_o_chunks} chunks\n\n' \
                              f'file size: {file_size / (1024 ** 2)} mebibytes \n\ntime: {duration} seconds'
                else:
                    message = f'Please be advised that {sfFile} has been imported into snowflake as the table {sfDatabase}.{str(SfSchema).lower()}.{sfTable_name}. \n \n' \
                              f'Success is {success} with {total_rows_loaded} rows loaded in {number_o_chunks} chunks\n\n' \
                              f'error log attached\n\n' \
                              f'file size: {file_size / (1024 ** 2)} mebibytes \n\ntime: {duration} seconds'
            Communication.send_mail(sender_email=sender_email, receiver_email=receiver_email, subject=subject,
                                    body=message, attachments=error_attatchment)
        except Exception as err_message:
            destinationKey = f"file2table/{sfDatabase}/failed_files/{sfFile}"
            S3Connection.s3Move(s3Resource=resource, s3DestinationBucket=s3Bucket, s3DestinationKey=destinationKey,
                                s3SourceBucket=s3Bucket, s3SourceKey=file)
            err_subject = f"{sfTable_name} Load Table Error"
            end = time.time()
            duration = end - start
            err_body = f"file: {sfFile} \ntable: \n{sfTable_name} \n\nsnowflake schema: \n{snowflakeSchemaDefinition}\n\n" \
                       f"Create Table SQL:\n{create_sql} \n\nerror: \n{err_message}  \n\n" \
                       f"Please make sure the file format is UTF-8\n\t- UTF-8-BOM is not supported \n\n" \
                       f"Delimiter Used: {delimiter}\n\nfile size: {file_size / (1024 ** 2)} mebibytes \n\ntime: {duration} seconds"
            Communication.send_mail(sender_email=err_sender_email, receiver_email=receiver_email, subject=err_subject,
                                    body=err_body, attachments=[])
        if os.path.isfile(f"{temp_folder}/{sfTable}_errors.txt"):
            os.remove(f"{temp_folder}/{sfTable}_errors.txt")


if __name__ == '__main__':
    ## Dev
    # dev_config_path = "/home/ec2-user/users/darwin_uy/filetotable/config/config_dev.yaml"
    # prod_config_path = "/home/ec2-user/users/darwin_uy/filetotable/config/config_prod.yaml"

    # ## Prod
    dev_config_path = "/home/ec2-user/production/filetotable/config_dev.yaml"
    prod_config_path = "/home/ec2-user/production/filetotable/config/config_prod.yaml"

    ## local
    # dev_config_path = "C:/Users/Darwin_Uyuy/PycharmProjects/Git projects/file2table/config/config_dev.yaml"
    # prod_config_path = "C:/Users/Darwin_Uyuy/PycharmProjects/Git projects/file2table/config/config_prod.yaml"

    s3_to_sf(config_yaml_path=prod_config_path)
    print("done")
