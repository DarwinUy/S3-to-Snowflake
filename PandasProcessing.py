#!/usr/bin/env python
# title           :PandasProcessing.py
# description     :This will create a header for a python script.
# author          :Darwin Uy
# date            :2021-11-11
# version         :0.1
# usage           :python pyscript.py
# notes           :
# python_version  :3.9
# ==============================================================================
import pandas as pd


def importToPandas(object, delimiter, header=True):
    ''' Description: This is a function that reads a file from S3 and imports it into pandas
        Input:
            object:
            delimeter:
            header:

        Output:
            Pandas Dataframe of S3 object
        '''
    if header == True:
        df = pd.read_csv(object['Body'], sep=delimiter, header=0)
    if header == False:
        df = pd.read_csv(object['Body'], sep=delimiter, header=None)
    return df


def pandasInferSchema(pandasDataframe):
    ''' Description: This is a function that infers the schema from a dataframe
        Input:
            pandas dataframe

        Output:
            same dataframe with types infered
            types that were inferred

        '''
    pandasDataframeInfered = pandasDataframe.infer_objects()
    pandasDataframeInferedTypes = pandasDataframe.infer_objects().dtypes
    return pandasDataframeInfered, pandasDataframeInferedTypes


def getSchemaPandas2Snowflake(pandasDataFrame):
    ''' Description: This is a function that creates a
        Input:
            pandasDataFrame

        Output:
            schema: string that defines the schema for table creation in Snowflake

        '''
    col_names = pandasDataFrame.columns.tolist()  # get columns
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
