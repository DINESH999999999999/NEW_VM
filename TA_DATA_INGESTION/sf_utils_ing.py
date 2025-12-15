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



def src_cnt(file_pattern, cloud_path, file_format_obj_name):
    print("LAKSHMI")

    if r's3://' in cloud_path:
        cloud_code='S3'
        storage_integration = r'S3_BUCKET'
        stage_object = r'DATAMIGRATION.DEMO_USER.S3_STAGE'
        
    elif r'azure://' in cloud_path:
        cloud_code='AZ'
        storage_integration = r'AZURE_BLOB_CONTAINER'
        stage_object = r'DATAMIGRATION.DEMO_USER.AZ_STAGE'

    query = f"DESC STORAGE INTEGRATION {storage_integration};"
    #query = f"""SELECT COUNT(*) FROM @{cloud_code}_{file_pattern}"""
    try:
        result=sfquery(query)
        
        root_path = result[2][2]
        act_path = cloud_path.replace(root_path,'')
        print(act_path,"Actual Path")
        returncode=0
    except Exception as e:
        returncode=1
        result=str(e)
        src_count=str(e)
        src_info=str(e) 

        return [returncode,src_count,src_info,'NA']

    try:
        query1 = f"""SELECT 
                METADATA$FILENAME AS FILE_NAME,
                COUNT(*) AS ROW_COUNT
                FROM @{stage_object}/{act_path} (PATTERN => '{file_pattern}' , FILE_FORMAT => {file_format_obj_name})
                GROUP BY METADATA$FILENAME
                ORDER BY FILE_NAME; """
        print(query1)
        result=sfquery(query1)
        
        src_count = sum(item[1] for item in result)
        print(src_count) 
        src_info = "FILE_PATH : ROW_COUNT\n \n"

        for i in result:
            src_info = src_info + f"{root_path}{i[0]} : {i[1]}\n"
        
        print(src_info)
        returncode=0
    except Exception as e:
        returncode=1
        src_count=str(e)
        src_info=str(e) 


    return [returncode,src_count,src_info,act_path]

#ds=src_cnt('.*SERVICE.*.csv','s3://tdsfbucket/TDEXPORT/PARQUET_FOLDER/')

#azure://snowflaketeradata213.blob.core.windows.net/teradataexport/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_INVENTORY_TPT_20250309_1501/
#azure://snowflaketeradata213.blob.core.windows.net/teradataexport/
#ds=src_cnt('.*part.*..parquet','s3://tdsfbucket/TDEXPORT/PARQUET_FOLDER/')


#returncode,src_count,src_info=src_cnt('.*part.*..parquet','azure://snowflaketeradata213.blob.core.windows.net/teradataexport/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_INVENTORY_TPT_20250309_1501/')

#print(returncode,src_count,src_info)

#sfquery("SELECT * FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE")


def create_file_format(job):
    create_stmt = ""
    job_id = job[0]
    batch_id = job[1]
    file_pattern = job[2]
    cloud_path = job[3]
    sf_database_name = job[4]
    sf_schema_name = job[5]
    sf_table_name = job[6]
    warehouse_name = job[7]
    load_mode = job[8]
    file_type = job[9]
    field_delimiter = job[10]
    field_optionally_enclosed_by = job[11]
    escape_character = job[12]
    skip_header = job[13]
    additional_file_format_options = job[14]
    file_format_obj_name = f"{file_type}_{batch_id}_{job_id}"
    if file_type == 'CSV':
        escape_character = escape_character.replace("\\", "\\\\")
        create_stmt = create_stmt + f"CREATE OR REPLACE FILE FORMAT {sf_database_name}.{sf_schema_name}.{file_type}_{batch_id}_{job_id} TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '{field_optionally_enclosed_by}', FIELD_DELIMITER = '{field_delimiter}', ESCAPE = '{escape_character}', SKIP_HEADER = {skip_header} "
        if additional_file_format_options != None:
            create_stmt = create_stmt + f", {additional_file_format_options}"
        create_stmt = create_stmt + ";"
    
    elif file_type == 'PARQUET':
        create_stmt = create_stmt + f"CREATE OR REPLACE FILE FORMAT {sf_database_name}.{sf_schema_name}.{file_type}_{batch_id}_{job_id} TYPE = 'PARQUET' "
        if additional_file_format_options != None:
            create_stmt = create_stmt + f", {additional_file_format_options}"
        create_stmt = create_stmt + ";"
    
    elif file_type == 'JSON':
        create_stmt = create_stmt + f"CREATE OR REPLACE FILE FORMAT {sf_database_name}.{sf_schema_name}.{file_type}_{batch_id}_{job_id} TYPE = 'JSON' "
        if additional_file_format_options != None:
            create_stmt = create_stmt + f", {additional_file_format_options}"
        create_stmt = create_stmt + ";"

    else:
        create_stmt = "NA"

    print(create_stmt)
    #print("SRIRAMA")
    try:
        result=str(sfquery(create_stmt))
        returncode=0
        status='SUCCESS'
    except Exception as e:
        returncode=1
        result=str(e)
        status='FAILED'
    
    print(returncode,create_stmt,result,status) 
    return [returncode,file_format_obj_name,create_stmt,result,status]


def ingestion(job,act_path,file_format_obj_name):
    #print("LAKSHMI")

    job_id = job[0]
    batch_id = job[1]
    file_pattern = job[2]
    cloud_path = job[3]
    sf_database_name = job[4]
    sf_schema_name = job[5]
    sf_table_name = job[6]
    warehouse_name = job[7]
    load_mode = job[8]
    file_type = job[9].lower()
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
    opt={"pattern" : f"{file_pattern}",
     "inferSchema" :"True",
     "format_name" : f"{sf_database}.{sf_schema}.{file_format_obj_name}"}

    print(opt)

    
    #df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")
    try:
        
        if file_type == 'csv':
            df_snowpark = spsession.read.options(opt).csv(f"@{stage_object}/{act_path}")
        
        elif file_type == 'parquet':
            df_snowpark = spsession.read.options(opt).parquet(f"@{stage_object}/{act_path}")
            print(df_snowpark)
        elif file_type == 'json':
            df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")

        #df_snowpark = spsession.read.options(opt).options(opt).load(f"@{stage_object}/{act_path}")

        df_snowpark.write.mode(f"{load_mode}").save_as_table(f"{sf_database}.{sf_schema}.{sf_table_name}")
        
        file_format_drop_stmt = f'DROP FILE FORMAT IF EXISTS {sf_database_name}.{sf_schema_name}.{file_format_obj_name};'
        try:
            result=str(sfquery(file_format_drop_stmt))
        except Exception as e:
            returncode=1
        
        his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                            from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
                            order by start_time DESC;""")
        history=his.collect()
        print('KAMALAKSHA',history[2][0] ,history[2][-3])
        ingestion_query=history[2][0]
        tar_cnt = history[2][-3]
        returncode = 0
        
        


        
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
        returncode = 1


    spsession.close()
    
    return [returncode,ingestion_query,tar_cnt]

def copy_ingestion(job,act_path,file_format_obj_name):
    print("LAKSHMI")
    #print("LAKSHMI")

    job_id = job[0]
    batch_id = job[1]
    file_pattern = job[2]
    cloud_path = job[3]
    sf_database_name = job[4]
    sf_schema_name = job[5]
    sf_table_name = job[6]
    target_table = f"{sf_database_name}.{sf_schema_name}.{sf_table_name}"
    warehouse_name = job[7]
    load_mode = job[8]
    file_type = job[9]
    additional_copy_options = job[15]
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
    
    #spsession=Session.builder.configs(spcon).create()
    #session_id = spsession.session_id
    if file_type == 'CSV':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path} FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} PATTERN = '{file_pattern}' """
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)
    
    elif file_type == 'PARQUET':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path} FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} PATTERN = '{file_pattern}'"""
        additional_copy_options = additional_copy_options + " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)
    
    elif file_type == 'JSON':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path} FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} PATTERN = '{file_pattern}'"""
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)

    else:
        copystmnt = "NA"
        print(copystmnt)
    #spsession.close()
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
        

def audit_entry(job):
    print("LAKSHMI")



def insert_audit_batch(batch_id):
    try:
        query=f"""INSERT INTO DATAMIGRATION.DEMO_USER.INGESTION_AUDIT_TABLE (JOB_ID, BATCH_ID, FILE_PATTERN, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_MODE, FILE_TYPE, SOURCE_INFO, FILE_FORMAT_OBJECT_STATEMENT, INGESTION_STATEMENT, SOURCE_COUNT, TARGET_COUNT, JOB_START_TIME, JOB_END_TIME, JOB_DURATION, FINAL_STATUS) SELECT JOB_ID, BATCH_ID, FILE_PATTERN, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_MODE, FILE_TYPE, SOURCE_INFO, FILE_FORMAT_OBJECT_STATEMENT, INGESTION_STATEMENT, SOURCE_COUNT, TARGET_COUNT, JOB_START_TIME, JOB_END_TIME, JOB_DURATION, FINAL_STATUS FROM DATAMIGRATION.DEMO_USER.INGESTION_LOG_TABLE WHERE BATCH_ID = {batch_id} ;"""
        result=str(sfquery(query))
        returncode=0  
    except Exception as e:
        returncode=1
        result=str(e)

def create_target_table(job,file_format_obj_name,act_path):
    print("Rama")
    job_id = job[0]
    batch_id = job[1]
    file_pattern = job[2]
    cloud_path = job[3]
    sf_database_name = job[4]
    sf_schema_name = job[5]
    sf_table_name = job[6]
    warehouse_name = job[7]
    load_mode = job[8]
    file_type = job[9].lower()
    table_exists = job[16]
    #print("RADHE GOVINDA")
    print(act_path,file_format_obj_name)

    if table_exists == 'YES' and load_mode == 'append':
        query=f"""SELECT TABLE_NAME FROM {sf_database_name}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{sf_schema_name}' AND TABLE_NAME = '{sf_table_name}';"""
        result=sfquery(query)
        print(result,len(result))
        if len(result)==1:
            result=f'Table {sf_database_name}.{sf_schema_name}.{sf_table_name} already exists.'
            returncode=0    
        else:
            result=f'Table {sf_database_name}.{sf_schema_name}.{sf_table_name} does not exist. Cannot proceed with APPEND load.'
            returncode=1
        return [returncode,result]
    
    elif table_exists == 'YES' and load_mode == 'overwrite':
        try:
            query=f"""TRUNCATE TABLE {sf_database_name}.{sf_schema_name}.{sf_table_name};"""

            result=str(sfquery(query))
            result = query + '\n' + str(result)
            returncode=0
            
        except Exception as e:
            query=f"""TRUNCATE TABLE {sf_database_name}.{sf_schema_name}.{sf_table_name};"""
            
            result=str(e)
            result = query + '\n' + str(result)
            returncode=1
        
        return [returncode,result]
    
    elif table_exists == 'NO':

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
        "format_name" : f"{sf_database_name}.{sf_schema_name}.{file_format_obj_name}"}

        print(opt)

        
        #df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")
        try:
            
            if file_type == 'csv':
                df_snowpark = spsession.read.options(opt).csv(f"@{stage_object}/{act_path}")
            
            elif file_type == 'parquet':
                df_snowpark = spsession.read.options(opt).parquet(f"@{stage_object}/{act_path}")
                print(df_snowpark)
            elif file_type == 'json':
                df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")

            #df_snowpark = spsession.read.options(opt).options(opt).load(f"@{stage_object}/{act_path}")
            load_mode='overwrite' #ALWAYS OVERWRITE FOR CREATE TABLE
            df_snowpark.print_schema()
            df_snowpark.columns
            empty_df = df_snowpark.limit(0)
            empty_df.write.mode(f"{load_mode}").save_as_table(f"{sf_database_name}.{sf_schema_name}.{sf_table_name}")
            
            his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                                from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
                                order by start_time DESC;""")
            history=his.collect()
            print('KAMALAKSHA',history[2][0] ,history[2][-3])
            create_table_query=history[2][0]
            tar_cnt = history[2][-3]
            returncode = 0
            
        except Exception as e:
            his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                                from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
                                order by start_time DESC;""")
            history=his.collect()
            print('KAMALANATHA',history[2][0] ,history[2][-3])
            create_table_query=history[2][0]
            tar_cnt = history[2][-3]
            #ingestion_query=ingestion_query
            tar_cnt=str(e)
            returncode = 1
        
        spsession.close()
        return [returncode,create_table_query]

def create_table(sfdatabasename,sfschemaname,sftablename,loadtype,schcol):
    print("JANAKIRAMA",sfdatabasename,sfschemaname,sftablename,schcol)
    if loadtype == 'CUSTOM_SQL':
        try:
            query=f"""CREATE OR REPLACE TABLE {sfdatabasename}.{sfschemaname}_WRK.{sftablename} ("""
            for i in schcol:
                c=i.strip()
                c=c.replace("\n"," ")
                c=c.replace("\t"," ")
                query= query + c 
            query1 = query +");"
            
            query2= query.replace(f"CREATE OR REPLACE TABLE {sfdatabasename}.{sfschemaname}_WRK.{sftablename}",f"CREATE OR REPLACE TABLE {sfdatabasename}.{sfschemaname}.{sftablename}")
            query2 = query2 +");"
            result=str(sfquery(query1))
            result=str(sfquery(query2))
            returncode=0

            print(query)
        except Exception as e:
            returncode=1
            result=str(e)
    else:
        try:
            query=f"""DELETE FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"""
        
            result=str(sfquery(query))
            returncode=0
            
        except Exception as e:
            returncode=1
            result=str(e)
    
    return [returncode,result]

 
def create_stage(sfdatabasename,sfschemaname,cloud_path):
    print("MAHALAKSHMI")

    if r's3://' in cloud_path:
        cloud_code='S3'
        storage_integration = r'S3_BUCKET'
        
    elif r'blob.core.windows.net' in cloud_path:
        cloud_code='AZ'
        storage_integration = r'AZURE_BLOB_CONTAINER'


    query=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = '{cloud_code}_{sfdatabasename}_{sfschemaname}';"""
    result=sfquery(query)
    if len(result)==1:
        print("Stage present")
        print(result)
        result=str(result[0][0]) + ' ALREADY EXISTS'
        stagename=f'{sfdatabasename}.{sfschemaname}.{cloud_code}_{sfdatabasename}_{sfschemaname}'
        returncode=0
    else:
        print(result)
        print("Stage Not present")
        stagename=f'{cloud_code}_{sfdatabasename}_{sfschemaname}'
        query1=f"""CREATE OR REPLACE STAGE {sfdatabasename}.{sfschemaname}.{stagename}
                URL='{cloud_path}'
                STORAGE_INTEGRATION = {storage_integration};"""
        print(query1)
        try:
            result=sfquery(query1)
            print(result)
            returncode=0
            stagename=f'{sfdatabasename}.{sfschemaname}.{cloud_code}_{sfdatabasename}_{sfschemaname}'
        except Exception as e:
            returncode=1
            result=str(e)
            stagename=f'{sfdatabasename}.{sfschemaname}.{cloud_code}_{sfdatabasename}_{sfschemaname}'
    

    return [returncode,result,stagename]
    '''
        return [returncode,f"""{sfdatabasename}.{sfschemaname}.{stagename}""",result]
    except Exception as e:
        returncode=1
        stagename=f'S3_{sfdatabasename}_{sfschemaname}'
        result=str(e)
        return [returncode,f"""{sfdatabasename}.{sfschemaname}.{stagename}""",result]
'''

def copycommand(stagename,jobdetails,uploadfilename):
    try:

        print("NARAYANA")
        print(jobdetails,uploadfilename)

        tddbname=jobdetails[0]
        tdtablename=jobdetails[1]
        sfdatabasename=jobdetails[2]
        sfschemaname=jobdetails[3]
        sftablename=jobdetails[4]
        delimiter=jobdetails[10]
        s3_path=jobdetails[14]
        uploadfoldername=uploadfilename.replace('.csv','')
        sfwrktable=sfdatabasename+'.'+sfschemaname+'_WRK'+'.'+sftablename

        '''
        uploadfilename='DEMO_USER_IOP_TPT_20250119_0818.csv'
        sfdatabasename='DATAMIGRATION'
        sfschemaname='DEMO_USER'
        sftablename='IOP'
        delimiter=','
        uploadfoldername=uploadfilename.replace('.csv','')
        sfwrktable=sfdatabasename+'.'+sfschemaname+'_WRK'+'.'+sftablename
        '''

        print(tddbname,tdtablename,s3_path,uploadfilename,sfdatabasename,sfschemaname,sftablename,delimiter,uploadfilename,uploadfoldername,sfwrktable)

        print("THIRUVIKRAMA")

        #stagename=create_stage(sfdatabasename,sfschemaname,s3_path)

        copystmnt=fr"""COPY INTO {sfwrktable} FROM @{stagename}/{uploadfoldername}/ FILE_FORMAT = ( TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = '{delimiter}' )"""

    
        result=str(sfquery(copystmnt))
        returncode=0
        
    except Exception as e:
        returncode=1
        result=str(e)
    
    return [returncode,copystmnt,result]

#copycommand(1,1)

#sfquery('test') 
#sfdatabasename='DATAMIGRATION'
#sfschemaname='DEMO_USER'

#qw=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = 'S3_{sfdatabasename}_{sfschemaname}';"""
#sd=sfquery(qw)

#a,b,c=create_stage('DATAMIGRATION','DEMO_USER','s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/')

#print(a,b,c)
'''
def mergecommand(job,export_start_time):
    print("RAGAVA")
    print(job,export_start_time)
''' 


def mergecommand(job):
    print("RAGAVA")
    print(job)
    try:
        sfdatabasename=job[2]
        sfschemaname=job[3]
        sftablename=job[4]
        filter=job[11]
        scd_type=job[6]
        load_type=job[7]
        custom_sql=job[17]
        primarykey=list(job[9].split(","))
        print(primarykey)

        if load_type=='FULL':
            print(load_type)
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            print(delstatement)
            print(insstatement)
            try:
                delreturn=sfquery(delstatement)
                insreturn=sfquery(insstatement)
                runstmnt = delstatement +"   \n" + insstatement
                returnstmnt=f"Number Of Records Deleted : {str(delreturn[0][0])} \n Number Of Records Inserted : {str(insreturn[0][0])}"
                returncode=0
            except Exception as e:
                returncode=1
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=str(e)


        elif load_type=='FILTER':
            if filter == None:
                filter = '(1=1)'

            if custom_sql != None:
                #try:
                filter = custom_sql[custom_sql.index('WHERE')+5:]
                #except Exception as f:
                #    filter = '(1=1)'
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename} WHERE {filter};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            print(delstatement)
            print(insstatement)
            try:
                delreturn=sfquery(delstatement)
                insreturn=sfquery(insstatement)
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=f"Number Of Records Deleted : {str(delreturn[0][0])} \n Number Of Records Inserted : {str(insreturn[0][0])}"
                returncode=0
            except Exception as e:
                returncode=1
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=str(e)
        
        
        elif load_type=='CUSTOM_SQL':
            print(load_type)
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            print(delstatement)
            print(insstatement)
            try:
                delreturn=sfquery(delstatement)
                insreturn=sfquery(insstatement)
                runstmnt = delstatement +"   \n" + insstatement
                returnstmnt=f"Number Of Records Deleted : {str(delreturn[0][0])} \n Number Of Records Inserted : {str(insreturn[0][0])}"
                returncode=0
            except Exception as e:
                returncode=1
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=str(e)
        
        elif load_type=='INCREMENTAL':
            
            if scd_type==0:
                insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
                try:
                    insreturn=sfquery(insstatement)
                    runstmnt=insstatement
                    returnstmnt=f"Number Of Records Inserted : {str(insreturn[0][0])}"
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt= insstatement
                    returnstmnt=str(e)

            if scd_type==1:
                pkcondition=""
                updstatement=""
                insstatement="("
                valstatement="("

                for i in primarykey:
                    t=f"TARGET.{i}=SOURCE.{i} AND "
                    pkcondition=pkcondition+t
                pkcondition=pkcondition[:-4]

                colliststatement=F"""SELECT COLUMN_NAME
                    FROM {sfdatabasename}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{sftablename}' and TABLE_SCHEMA='{sfschemaname}' AND TABLE_CATALOG='{sfdatabasename}'
                    ORDER BY ORDINAL_POSITION;"""    
                

                colreturn=sfquery(colliststatement)
                collist=[]
                
                for i in colreturn:
                    collist.append(i[0])
                
                print("Narayana",collist)

                for i in collist:
                    t=f"TARGET.{i}=SOURCE.{i}, "
                    r=f"{i}, "
                    e=f"SOURCE.{i}, "
                    updstatement=updstatement+t
                    insstatement=insstatement+r
                    valstatement=valstatement+e
                updstatement=updstatement[:-2]
                insstatement=insstatement[:-2]+')'
                valstatement=valstatement[:-2]+')'

                merstatement=f"""MERGE INTO {sfdatabasename}.{sfschemaname}.{sftablename} AS TARGET USING {sfdatabasename}.{sfschemaname}_WRK.{sftablename} AS SOURCE ON 
                    {pkcondition}
                    WHEN MATCHED THEN UPDATE SET 
                    {updstatement}
                    WHEN NOT MATCHED THEN INSERT 
                    {insstatement}
                    VALUES
                    {valstatement} ;
                    """
                print(merstatement)
                try:
                    merreturn=sfquery(merstatement)
                    runstmnt=merstatement
                    returnstmnt=f"""Number Of Records Inserted : {str(merreturn[0][0])} \n Number of Records Updated : {str(merreturn[0][1])}"""
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt=merstatement
                    returnstmnt=str(e)



            if scd_type==2:
                pkcondition=""
                updstatement=""
                insstatement="("
                valstatement="("

                for i in primarykey:
                    t=f"TARGET.{i}=SOURCE.{i} AND "
                    pkcondition=pkcondition+t
                pkcondition=pkcondition[:-4]

                colliststatement=F"""SELECT COLUMN_NAME
                    FROM {sfdatabasename}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{sftablename}' and TABLE_SCHEMA='{sfschemaname}' AND TABLE_CATALOG='{sfdatabasename}'
                    ORDER BY ORDINAL_POSITION;"""    
                
                colreturn=sfquery(colliststatement)
                collist=[]
                
                for i in colreturn:
                    collist.append(i[0])
                
                print("Narayana",collist)

                for i in collist:
                    t=f"TARGET.{i}=SOURCE.{i}, "
                    r=f"{i}, "
                    e=f"SOURCE.{i}, "
                    updstatement=updstatement+t
                    insstatement=insstatement+r
                    valstatement=valstatement+e
                updstatement=updstatement[:-2]
                insstatement=insstatement[:-2]+')'
                valstatement=valstatement[:-2]+')'

                merstatement=f"""MERGE INTO {sfdatabasename}.{sfschemaname}.{sftablename} AS TARGET USING {sfdatabasename}.{sfschemaname}_WRK.{sftablename} AS SOURCE ON 
                    {pkcondition}
                    WHEN MATCHED THEN UPDATE SET 
                    {updstatement}
                    WHEN NOT MATCHED THEN INSERT 
                    {insstatement}
                    VALUES
                    {valstatement} ;
                    """
                print(merstatement)
                try:
                    merreturn=sfquery(merstatement)
                    runstmnt=merstatement
                    returnstmnt=f"""Number Of Records Inserted : {str(merreturn[0][0])} \n Number of Records Updated : {str(merreturn[0][1])}"""
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt=merstatement
                    returnstmnt=str(e)
        return [returncode,runstmnt,returnstmnt]
    except Exception as e:
        returncode=1
        result=str(e)
        runstmnt=""
        print("AACHUTHA")

        return [returncode,runstmnt,returnstmnt]

def getcdcdates(tddbname,tdtablename):
    query2=f"""SELECT CAST(EXTRACTSTARTDTTM AS VARCHAR) AS EXTRACTSTARTDTTM,CAST(EXTRACTENDDTTM AS VARCHAR) AS EXTRACTENDDTTM FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE WHERE TD_DATABASE_NAME='{tddbname}' and TD_TABLE_NAME='{tdtablename}'"""
    result=sfquery(query2)
    return result

'''
sd=getcdcdates('DEMO_USER','IOP')
print(sd)
'''

def auditupdate(job,export_start_time):

    print(job,export_start_time)
    tddbname=job[0]
    tdtablename=job[1]

    audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE TD_DATABASE_NAME='{tddbname}' AND TD_TABLE_NAME='{tdtablename}';"""
    try:
        result=sfquery(audit_query)
        auditstmnt=audit_query
        returnstmnt=f"Number Of Records Updated : {str(result[0][0])}"
        returncode=0
    except Exception as e:
        returncode=1
        auditstmnt=audit_query
        returnstmnt=str(e)
    
    print(audit_query)
    return [returncode,auditstmnt,returnstmnt]

'''
export_start_time='2025-02-06 09:08:04.110000'
tdtablename='IOP'
tddbname='DEMO_USER'
audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE TD_DATABASE_NAME='{tddbname}' AND TD_TABLE_NAME='{tdtablename}';"""
print(audit_query)
'''


def sfcount(sfdbname,sfschname,sftablename):
    #query2=f"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26));"
    #job_end_time=tdquery(query2)[0][0]
    #job_end_time=str(datetime.now() - timedelta(hours=5))
    try:
        query1=f"SELECT COUNT(*) FROM {sfdbname}.{sfschname}_WRK.{sftablename};"
        sfcnt=sfquery(query1)[0][0]
        returncode=0
    except Exception as e:
        returncode=1
        sfcnt=str(e)
        

    return [returncode,sfcnt]
