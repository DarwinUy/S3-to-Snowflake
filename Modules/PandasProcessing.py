# title           :PandasProcessing.py
# description     :This will create a header for a python script.
# author          :Darwin Uy
# date            :2022-6-2
# version         :0.1
# usage           :python pyscript.py
# notes           :
# python_version  :3.9
# ==============================================================================
import pandas as pd


def importToPandas(object, delimiter, header=True):
    """
    Description
    -----------
    This is a function that reads a file from S3 and imports it into pandas

    Args
    ----
    object : object
        S3 object
    delimeter : string
        character used to beginning and end of the field
    header : int
        indicates the header row of the file

    Returns
    -------
    df : object
        Pandas Dataframe of S3 object
    """
    if header == True:
        df = pd.read_csv(object['Body'], sep=delimiter, header=0)
    if header == False:
        df = pd.read_csv(object['Body'], sep=delimiter, header=None)
    return df


def pandasInferSchema(pandasDataframe):
    """
    Description
    -----------
    This is a function that infers the schema from a dataframe

    Args
    ----
    pandasDataframe : object
        a pandas dataframe

    Returns
    -------
    pandasDataframeInfered : object
        dataframe with types infered
    pandasDataframeInferedTypes : list
        list of data types that were inferred
    """
    pandasDataframeInfered = pandasDataframe.infer_objects()
    pandasDataframeInferedTypes = pandasDataframe.infer_objects().dtypes
    return pandasDataframeInfered, pandasDataframeInferedTypes


def getSchemaPandas2Snowflake(pandasDataFrame):
    """
    Description
    -----------
    This program converts the pandas schema to snowflake and writes the string that defines a snowflake table

    Args
    ----
    pandasDataFrame : object
        pandas dataframe to have the schema extracted and converted

    Returns
    -------
    schema : string
        string that defines the schema for table creation in Snowflake
    """
    col_names = pandasDataFrame.columns.tolist()  # get columns
    # col_names = [f'\"{x}\"' for x in col_names]  # see if can add
    col_types = pandasDataFrame.dtypes.values.tolist()  # get list of dtypes
    col_types = [str(x) for x in col_types]  # convert from dtype to string
    col_types = [x + ',' for x in col_types]  #
    col_types = [sub.replace('object', 'VARCHAR(16777216)') for sub in col_types]
    col_types = [sub.replace('int64', 'INT') for sub in col_types]
    col_types = [sub.replace('float64', 'FLOAT8') for sub in col_types]
    col_types = [sub.replace('bool', 'BOOLEAN') for sub in col_types]
    schema = list(zip(col_names, col_types))
    schema = [item for t in schema for item in t]
    schema = ' '.join(schema)
    schema = schema[:len(schema) - 1]
    return schema
