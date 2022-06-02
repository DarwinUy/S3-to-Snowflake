import sys
import yaml
import pandas
sys.path.insert(0, '/home/ec2-user/production/S3_to_Snowflake/source')

import Communication
import S3Connection
import SnowflakeConnection
import PandasProcessing

with open(r'/home/ec2-user/production/S3_to_Snowflake/config/S3_Configurations.yaml') as file:
    s3Config = yaml.load(file, Loader=yaml.FullLoader)

with open(r'/home/ec2-user/production/S3_to_Snowflake/config/Snowflake_Configurations.yaml') as file:
    snowflakeConfig = yaml.load(file, Loader=yaml.FullLoader)

# Get Credentials
# Snowflake
SfSchema = snowflakeConfig['sf.schema']
SfPassphrase = snowflakeConfig['sf.passphrase']
SfKeyfile = snowflakeConfig['p8.key.file']
sfUser = snowflakeConfig['sf.user']
sfAccount = snowflakeConfig['sf.account']
sfWarehouse = snowflakeConfig['sf.warehouse']
sfPrivateKey = SnowflakeConnection.getPrivateKey(keyFile=SfKeyfile, snowflakePassword=SfPassphrase)
sfRole = 'SYS_SOURCE'

# Email

files = []
sfConn = SnowflakeConnection.createSnowflakeConnection(sfAccount = sfAccount, sfUser = sfUser, sfPrivateKey = sfPrivateKey, sfWarehouse = 'USERS_DATA', sfDatabase = 'INTERNATIONAL', sfSchema = 'PROCESSED')
sfCur = sfConn.cursor()
# Get processed CSV list
sql = "SELECT DISTINCT FILENAME FROM INTERNATIONAL.PROCESSED.UP_CSV_OUTPUT_ARCHIVE order by FILENAME"
sfCur.execute(sql)
for record in sfCur:
    files.append(str(record[0]))
# Get processed XLSX list
sql = "SELECT DISTINCT FILENAME FROM INTERNATIONAL.PROCESSED.UP_XLSX_OUTPUT_ARCHIVE order by FILENAME"
sfCur.execute(sql)
for record in sfCur:
    files.append(str(record[0]))
files = '\n'.join(files)
print(files)

subject = "UP processing completed"
message = f"Please be advised that the files: \n\n{files} \n\nhave been processed"

Communication.send_mail(sender_email=sender_email, receiver_email=receiver_email, subject=subject,
                        body=str(message), attachments=[])

