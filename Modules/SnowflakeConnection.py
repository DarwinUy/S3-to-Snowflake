# title           :SnowflakeConnection.py
# description     :Module to perform functions regarding Snowflake
# author          :Darwin Uy
# date            :2022-6-2
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
    """
    Description
    -----------
    This is a function that decrypts a Snowflake private key

    Args
    ----
    keyFile: string
        path the Snowflake rsa p8 key file
    snowflakePassword: string
        a Snowflake Passphrase

    Returns
    -------
    dkey: object
        The decrypted snowflake private key
    """
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
    """
    Description
    -----------
    Create a Snowflake connection instance

    Args
    ----
    sfUser: string
        Snowflake User Name
    sfAccount: string
        Snowflake account
    sfPrivateKey: object
        This is the decrypted private key
        - Output from getPrivateKey(keyFile, snowflakePassword)
    sfWarehouse: string
        Snowflake Warehouse to be used
    sfDatabase: string
        Snowflake Database to be used
    sfSchema: string
        Snowflake Schema to be used

    Returns
    -------
    connection: object
        A Snowflake connection instance
    """
    connection = snowflake.connector.connect(user=sfUser,
                                             account=sfAccount,
                                             private_key=sfPrivateKey,
                                             warehouse=sfWarehouse,
                                             database=sfDatabase,
                                             schema=sfSchema)
    return connection


def createSnowflakeTable(sfConn, sfRole, sfDatabase, sfSchema, sfTable, tableSchemaDef, insert=True):
    """
    Description
    -----------
    Creates a new table in in Snowflake

    Args
    ----
    sfConn: object
        Snowflake connection instance
    sfRole: string
        Snowflake Role
    sfDatabase: string
        Snowflake Database to be used
    sfSchema: string
        Snowflake Schema to be used
    sfTable: string
        Snowflake Table to be used
    tableSchemaDef: string
        Schema definition for a Snowflake table
    insert: Bool
        Whether to insert or not

    Returns
    -------
    None
    """
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
    return sql


def writePandas2Snowflake(sfConn, pdDF, sfTable):
    """
    Description
    -----------
    A function that writes from a pandas dataframe to a Snowflake table

    Args
    ----
        conn: object
            snowflake connection
        pdDF: object
            pandas dataframe to be written from
        sfTable: string
            designated snowflake table

    Returns
    -------
    success: bool
        Indicates whether write was successful or not
    nchunks: int
        number of chunks used to load table
    nrows: int
        number of rows loaded
    """
    # Writes pandas DF to Snowflake
    print("PD to Snowflake")
    success, nchunks, nrows, _ = write_pandas(sfConn, pdDF, sfTable, quote_identifiers=False)
    print(f"Success is {success} with {nrows} rows loaded")
    return (success, nchunks, nrows)
