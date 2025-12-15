import snowflake.connector
import json
#from td_utils import tdquery
from datetime import datetime, timedelta
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

def sfquery(query):
    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = cred['sf_warehouse']
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']
    #print("RAMA")            

    sfcon = snowflake.connector.connect(
        account=sf_host ,
        user=sf_user, 
        password=sf_password,
        database=sf_database,
        schema=sf_schema,
        warehouse=sf_warehouse)
    
    query=query
     
    print(query)

    with sfcon.cursor() as curr:
        curr.execute(query)
        result=curr.fetchall()
    print(result,type(result))
    return result


def ingestion(job,act_path,file_format_obj_name):
    #print("LAKSHMI")

    #job_id = job[0]
    #batch_id = job[1]
    file_pattern = '.*ranga.*'
    cloud_path = 's3://snowflakebucketta/exports/PARQ/'
    sf_database_name = 'DATAMIGRATION'
    sf_schema_name = 'DEMO_USER'
    sf_table_name = 'PARQ'
    warehouse_name = 'COMPUTE_WH'
    load_mode = 'overwrite'  #'append'  #'overwrite'
    file_type = 'parquet'  #'csv'  #'json'
    print("RADHE GOVINDA")
    file_format_obj_name = 'PARQUET_FF'
    print(file_format_obj_name)

    if r's3://' in cloud_path:
        cloud_code='S3'
        stage_object = r'DATAMIGRATION.DEMO_USER.S3_STAGE'
        
    elif r'azure://' in cloud_path:
        cloud_code='AZ'
        stage_object = r'DATAMIGRATION.DEMO_USER.AZ_STAGE'
    
    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = warehouse_name
    sf_database = sf_database_name
    sf_schema = sf_schema_name

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
    session_id = spsession.session_id
    opt={"pattern" : f"{file_pattern}",
     "inferSchema" :"True",
     "format_name" : f"{sf_database}.{sf_schema}.{file_format_obj_name}"}

    print(opt)  
    act_path = 'PART'
    
    #df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")
    #try:

    if file_type == 'csv':
        df_snowpark = spsession.read.options(opt).csv(f"@DATAMIGRATION.DEMO_USER.S3_STAGE/PARQ/")

    elif file_type == 'parquet':
        df_snowpark = spsession.read.options(opt).parquet(f"@DATAMIGRATION.DEMO_USER.S3_STAGE/PARQ/")
        print(df_snowpark)
    elif file_type == 'json':
        df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")

    #df_snowpark = spsession.read.options(opt).options(opt).load(f"@{stage_object}/{act_path}")
    df_snowpark.print_schema()
    df_snowpark.columns
    empty_df = df_snowpark.limit(0) # same schema, no rows 
    #empty_df.write.save_as_table("my_table", mode="append")
    empty_df.write.mode(f"overwrite").save_as_table(f"{sf_database}.{sf_schema}.{sf_table_name}")



    his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                        from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
                        order by start_time DESC;""")
    history=his.collect()
    print('KAMALAKSHA',history[2][0] ,history[2][-3])
    ingestion_query=history[2][0]
    tar_cnt = history[2][-3]
    returncode = 0

        


    '''
    except Exception as e:

        his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                            from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
                            order by start_time DESC;""")
        history=his.collect()
        print('KAMALANATHA',history[2][0] ,history[2][-3])
        ingestion_query=history[2][0]
        tar_cnt = history[2][-3]
        #ingestion_query=ingestion_query
        tar_cnt=str(e)
        returncode = 1'''

    spsession.close()
    
    return [returncode,ingestion_query,tar_cnt]

#ingestion(1,2,3)

def copy_ingestion(job,act_path,file_format_obj_name):
    print("LAKSHMI")
    #print("LAKSHMI")
    file_format_obj_name = 'PARQUET_FF'
    act_path = 'ORDERS'
    file_pattern = '.*orders.*'
    cloud_path = 's3://snowflakebucketta/exports/ORDERS'
    sf_database_name = 'DATAMIGRATION'
    sf_schema_name = 'DEMO_USER'
    sf_table_name = 'ORDERS'
    target_table = f"{sf_database_name}.{sf_schema_name}.{sf_table_name}"
    warehouse_name = 'COMPUTE_WH'
    file_type = 'PARQUET'
    additional_copy_options = 'ON_ERROR = CONTINUE FORCE = TRUE'
    #print("RADHE GOVINDA")
    print(act_path,file_format_obj_name)
     
    if r's3://' in cloud_path:
        cloud_code='S3'
        stage_object = r'DATAMIGRATION.DEMO_USER.S3_STAGE'
        
    elif r'azure://' in cloud_path:
        cloud_code='AZ'
        stage_object = r'DATAMIGRATION.DEMO_USER.AZ_STAGE'
    
    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = warehouse_name
    sf_database = sf_database_name
    sf_schema = sf_schema_name

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
    session_id = spsession.session_id
    if file_type == 'CSV':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path}/ FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} """
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)
    
    elif file_type == 'PARQUET':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path}/ FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} """
        additional_copy_options = additional_copy_options + " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)
    
    elif file_type == 'JSON':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path}/ FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} """
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)

    else:
        copystmnt = "NA"
        print(copystmnt)

    try:
        result=sfquery(copystmnt)
        print(result)
        returncode = 0
        ingestion_cnt = 0
        print(result,returncode,copystmnt)
        for row in result:
            ingestion_cnt = ingestion_cnt + row[2]
        return [returncode,copystmnt,result,ingestion_cnt]

    except Exception as e:
        returncode=1
        result=str(e)
        ingestion_cnt = 0
        return [returncode,copystmnt,result,ingestion_cnt]

print(copy_ingestion(1,2,3))

#sfquery("select current_versiosdsdn()")