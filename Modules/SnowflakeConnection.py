#!/usr/bin/env python
# title           :pyscript.py
# description     :Module to perform functions regarding Snowflake
# author          :Darwin Uy
# date            :2021-04-11
# version         :0.1
# usage           : Module for Snowflake related functions
# notes           :
# python_version  :3.9
# ==============================================================================
# Connectors
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Encryption
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


# Get Private Key
def getPrivateKey(keyFile, snowflakePassword):
    ''' Description: This is a function that decodes the private key
        Input:
            keyFile: The location of the Snowflake rsa p8 key file
            snowflakePassword: The Snowflake Passphrase
        Output:
            dkey: The decrypted snowflake private key
        '''

    with open(keyFile, "r") as keyfile:
        pkey = keyfile.read()
    key = serialization.load_pem_private_key(pkey.encode(),
                                             password=snowflakePassword.encode(),
                                             backend=default_backend())

    dKey = key.private_bytes(encoding=serialization.Encoding.DER,
                             format=serialization.PrivateFormat.PKCS8,
                             encryption_algorithm=serialization.NoEncryption())

    return dKey


def createSnowflakeConnection(sfAccount, sfUser, sfPrivateKey, sfWarehouse, sfDatabase, sfSchema):
    ''' Description: This is a function that
        Input:
            sfUser: Snowflake User Name
            sfAccount: Snowflake account
            sfPrivateKey: This is the decrypted private key
                - Output from getPrivateKey(keyFile, snowflakePassword)
            sfWarehouse: Snowflake Warehouse to be used
            sfDatabase: Snowflake Database to be used
            sfSchema: Snowflake Schema to be used
        Output:
            A Snowflake connection instance
        '''
    connection = snowflake.connector.connect(user=sfUser,
                                             account=sfAccount,
                                             private_key=sfPrivateKey,
                                             warehouse=sfWarehouse,
                                             database=sfDatabase,
                                             schema=sfSchema)
    return connection


def createSnowflakeTable(sfConn, sfRole, sfDatabase, sfSchema, sfTable, tableSchemaDef, insert=True):
    ''' Description: This is a function that
        Input:
            sfConn: Snowflake connection instance
            sfRole: Snowflake Role
            sfDatabase: Snowflake Database
            sfSchema: Snowflake Schema
            sfTable: Snowflake Table
            tableSchemaDef: Schema definition for a Snowflake table
        Result:
            Newly created Snowflake table
        '''
    cur = sfConn.cursor()

    # Select Role
    sql = f"USE ROLE {sfRole}"  # Specify role
    cur.execute(sql)

    # Select Database
    sql = f"USE DATABASE {sfDatabase}"  # from the folder name
    cur.execute(sql)

    # Select Schema
    sql = f"USE SCHEMA {sfSchema}"
    cur.execute(sql)

    # Create Table
    if insert == False:
        sql = f"CREATE OR REPLACE TABLE {sfDatabase}.{sfSchema}.{sfTable} ({tableSchemaDef})"
    if insert == True:
        sql = f"CREATE TABLE IF NOT EXISTS {sfDatabase}.{sfSchema}.{sfTable} ({tableSchemaDef})"  # for if inserting
    cur.execute(sql)
    print(f'{sfDatabase}.{sfSchema}.{sfTable} created')


def writePandas2Snowflake(sfConn, pdDF, sfTable):
    ''' Description: This is a function that
        Input:
            conn: snowflake connection
            pdDF: pandas dataframe to be written
            sfTable: designated snowflake table
        Result:
            Writes pandas dataframe to snowflake
        '''
    # Writes pandas DF to Snowflake
    print("PD to Snowflake")
    success, nchunks, nrows, _ = write_pandas(sfConn, pdDF, sfTable, quote_identifiers=False)
    print(f"Success is {success} with {nrows} rows loaded in {nchunks} chunks")
    return(success, nchunks, nrows)