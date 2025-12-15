import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json

with open('/media/ssd/python/credentials.json','r+') as config_file:
    cred=json.load(config_file)

sf_host = cred['sf_host']
sf_user = cred['sf_user']
sf_password = cred['sf_password']
sf_warehouse = cred['sf_warehouse']
sf_database = cred['sf_database']
sf_schema = cred['sf_schema']

spcon = {
"account": sf_host,
"user": sf_user,
"password": sf_password,
"warehouse": sf_warehouse,
"database": sf_database,
"schema":sf_schema
}

print(spcon)

spsession=Session.builder.configs(spcon).create()
'''batch=spsession.sql("LIST @DATAMIGRATION.DEMO_USER.AZ_STAGE")
print(batch.show())
s=batch.collect()
print(type(s))
print(len(s))  '''

#part-00001-0fa20b56-49b4-4b08-b16e-246101108fc6-c000.snappy.parquet

#pq = spsession.read.parquet('@DATAMIGRATION.DEMO_USER.AZ_STAGE/',options = {"pattern":'.*1b4f84b2-87a8-4469-bb00-08dbf0b9d680-c000.*'})

'''
df = spsession.read.parquet(
    "@DATAMIGRATION.DEMO_USER.AZ_STAGE/.*1b4f84b2-87a8-4469-bb00-08dbf0b9d680-c000.*",
    options={
        "pattern": ".*1b4f84b2-87a8-4469-bb00-08dbf0b9d680-c000.*" 
    }
)
'''


# Read CSV files matching a pattern
#df_pattern = spsession.read.options({"pattern" , ".*part.*.parquet"}).parquet("@DATAMIGRATION.DEMO_USER.AZ_STAGE/TDEXPORT/DATAMIGRATION/DEMO_USER/")
'''
df_test = spsession.read.parquet("@DATAMIGRATION.DEMO_USER.AZ_STAGE/TDEXPORT/DATAMIGRATION/service_now.parquet")

sch = df_test.schema

print(sch)
'''


opt={"pattern" : ".*SERVICE.*.csv",
     "inferSchema" :"True",
     "format_name" : "DATAMIGRATION.DEMO_USER.SCV"}
    # "storage_integration": "DATAMIGRATION.DEMO_USER.AZURE_BLOB_CONTAINER",
    # "path": "azure://snowflaketeradata213.blob.core.windows.net/teradataexport/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_SERVICE_NOW_TPT_20250316_0050/"}
     #"path": "azure://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_SERVICE_NOW_TPT_20250316_0050/"


opt={"pattern" : ".*HR.*.json",
     "inferSchema" :"True",
     "format_name" : "DATAMIGRATION.DEMO_USER.JSON_10028_9001"}
#"azure://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_SERVICE_NOW_TPT_20250316_0050/"
#df_pattern = spsession.read.options(opt).csv("@DATAMIGRATION.DEMO_USER.AZ_STAGE/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_SERVICE_NOW_TPT_20250316_0050/")

#df_pattern = spsession.read.options(opt).csv("@AZ_STAGE/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_SERVICE_NOW_TPT_20250316_0050/")


df_pattern = spsession.read.options(opt).json("@s3_stage/JSON_FOLDER/")
#https://snowflaketeradata213.blob.core.windows.net/teradataexport/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_SERVICE_NOW_TPT_20250316_0050/DEMO_USER_INVENTORY_TPT_20250309_1501-1-1.csv




print("RANGA")
print(df_pattern.schema)
print("RAMA")
#d=df_pattern.collect()
session_id = spsession.session_id
print(session_id)
#print(d)
#print(len(d))

#print(d)
df_pattern.write.mode("overwrite").save_as_table("DATAMIGRATION.DEMO_USER.json_table111")

his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
order by start_time DESC;""")

history=his.collect()
#print(history)

print("SriRama")

print(history[2])
spsession.close()