# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 15:10:59 2024

@author: DINESH_MALLIKARJUNAN
"""
import os
import subprocess
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import threading
import snowflake.connector
#import teradatasql
import pandas as pd
#from cloud_utils import cloud_upload
from sf_utils_ing import create_table,create_stage,copycommand,getcdcdates,mergecommand,auditupdate,sfcount,src_cnt,create_file_format,ingestion,insert_audit_batch
from logger_ing import batch_create,log_update
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from threading import Lock
import multiprocessing
import shutil
import json

thread_local_data = threading.local()
lock = multiprocessing.Lock()
#lock = threading.Lock()
#lock = Lock()

print("kasava") 


def dataingest(task):
    time.sleep(1)
    rc_sum=0
    job=task
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
        
    print(job_id, batch_id, file_pattern, cloud_path, sf_database_name, sf_schema_name, sf_table_name, warehouse_name, load_mode, file_type, field_delimiter, field_optionally_enclosed_by, escape_character, skip_header, additional_file_format_options)
    
    
    
    print("MADHUSUDHANA")

    returncode,file_format_obj_name,file_format_obj_stmt ,file_format_obj_log, file_format_obj_status = create_file_format(job)
    rc_sum=rc_sum+returncode
    log_update('create_file_format',[returncode,file_format_obj_stmt ,file_format_obj_log, file_format_obj_status],batch_id,job_id)
    
    if returncode != 0:
        return sf_table_name 

    print("SriRama")
    returncode,src_count,src_info,act_path = src_cnt(file_pattern,cloud_path,file_format_obj_name)
    rc_sum=rc_sum+returncode
    print(returncode,src_count,src_info)

    log_update('src_cnt',[returncode,src_count,src_info],batch_id,job_id)

    if returncode != 0:
        return sf_table_name 

    print("KASAVA")

    returncode,ingestion_stmt,ingestion_cnt = ingestion(job,act_path,file_format_obj_name)
    rc_sum=rc_sum+returncode
    print(returncode,ingestion_stmt,ingestion_cnt)

    log_update('ingestion',[returncode,ingestion_stmt,ingestion_cnt],batch_id,job_id)

    if returncode != 0:
        return sf_table_name
    

    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)

    return sf_table_name

'''
    returncode,errormsg,tptfilename,tptcontent,exportfilename,export_start_time,colstr=tpt_script_generator(job)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('tpt_script_generator',[returncode,errormsg,tptfilename,tptcontent],batch_id,job_id)
    
    if returncode != 0:
        return sftablename    


    print("MADHUSUDHANA")
    print(returncode,errormsg)
    print("KASAVA")
    
    returncode,tpt_cmd,stdout=tptexport(tptfilename,exportfilename,job)
    if returncode == 4:
        returncode = 0
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('tptexport',[returncode,tpt_cmd,stdout],batch_id,job_id)

    if returncode != 0:
        return sftablename

    returncode,tdcnt=tdcount(stdout)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum,tdcnt)
    log_update('tdcount',[returncode,tdcnt],batch_id,job_id)
    
    if returncode != 0:
        return sftablename
    

    print("MADHAVA")
    uploadfilename=exportfilename.replace('.csv','')
    print(f"CLOUD UPLOAD STARTED FOR : {cloud_path},{uploadfilename}")
    print("GOVINDA")
    
    returncode,upload_cmd,cloud_log=cloud_upload(cloud_path,uploadfilename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('s3upload',[returncode,upload_cmd,cloud_log],batch_id,job_id)
    
    if returncode != 0:
        return sftablename
    
    print(f"CLOUD UPLOAD COMPLETED FOR :{cloud_path},{uploadfilename}")
    print("SRINIVASA")
    
    returncode,result=create_table(sfdbname,sfschname,sftablename,loadtype,colstr)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print("MADHAVA","MADHAVA",result)

    log_update('create_table',[returncode,result],batch_id,job_id)
    
    if returncode != 0:
        return sftablename    

    print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename,uploadfilename)
    
    print("KODANDAPANI")

    print("CREAT STAGE STARTED FOR:",sftablename)

    returncode,log,stagename=create_stage(sfdbname,sfschname,cloud_path)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('create_stage',[returncode,log,stagename],batch_id,job_id)

    if returncode != 0:
        return sftablename

    print("CREAT STAGE COMPLETED FOR:",sftablename)

    print("VARADHA")
    print("COPY COMMAND STARTED FOR :",sftablename)
    
    returncode,copystmnt,result=copycommand(stagename,job,uploadfilename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print(returncode,copystmnt,result)


    log_update('copycommand',[returncode,copystmnt,result],batch_id,job_id)
    
    if returncode != 0:
        return sftablename

    print("COPY COMMAND COMPLETED FOR :",sftablename)
    print("RANGA")
    
    print("SRIMATHA")
    print("MERGE STATEMENT STARTED FOR :",sftablename)
    returncode,merstmnt,result=mergecommand(job)
    print("RC_SUM",rc_sum)
    print("MERGE STATEMENT COMPLETED FOR :",sftablename)
    rc_sum=rc_sum+returncode
    log_update('mergecommand',[returncode,merstmnt,result],batch_id,job_id)
    
    if returncode != 0:
        return sftablename

    print("Srirama")
    print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,export_start_time)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print("AUDIT UPDATE STARTED FOR :",sftablename)

    log_update('auditupdate',[returncode,auditstmnt,result],batch_id,job_id)

    if returncode != 0:
        return sftablename

    returncode,sfcnt=sfcount(sfdbname,sfschname,sftablename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('sfcount',[returncode,sfcnt],batch_id,job_id)

    if returncode != 0:
        return sftablename

    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)
    '''
     

if __name__ == "__main__":
    print("SriRama")
    start=time.time()
    #'/media/ssd/python/credentials.json'

    #C:\Users\dines\Downloads\GCP_VM\python\credentials.json
    with open(r'C:\Users\dines\Pictures\NEW_VM\dataingestion\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = cred['sf_warehouse']
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']

    sfcon = snowflake.connector.connect(
        account=sf_host ,
        user=sf_user, 
        password=sf_password,
        database=sf_database,
        schema=sf_schema,
        warehouse=sf_warehouse)
    
    spcon = {
    "account": sf_host,
    "user": sf_user,
    "password": sf_password,
    "warehouse": sf_warehouse,
    "database": sf_database,
    "schema": sf_schema
    }



    query="""SELECT JOB_ID, (SELECT COALESCE((SELECT MAX(CAST(BATCH_ID AS INT)) FROM DATAMIGRATION.DEMO_USER.INGESTION_LOG_TABLE)+1,10000)) AS BATCH_ID, FILE_PATTERN, CLOUD_PATH, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, WAREHOUSE_NAME, LOAD_MODE, FILE_TYPE, FIELD_DELIMITER, FIELD_OPTIONALLY_ENCLOSED_BY, ESCAPE_CHARACTER, SKIP_HEADER, ADDITIONAL_FILE_FORMAT_OPTIONS FROM DATAMIGRATION.DEMO_USER.INGESTION_CONFIG_TABLE;"""
    
    spsession=Session.builder.configs(spcon).create()
    batch=spsession.sql(query)
    #print("Sridhara")
    
    config=batch.collect()
    configtable=list(config)

    try:
        batch_create()
    except Exception as e:
        print(e)
        print("NOT ABLE TO CREATE LOG")
        exit()

    


    
    with ProcessPoolExecutor() as executor:
        status_code_tpt_scr_gen = {executor.submit(dataingest, task): task for task in configtable}
        for return_code in as_completed(status_code_tpt_scr_gen):
            print(return_code.result(),"Return Code")
    
    return_code=insert_audit_batch(configtable[0][1])

    print("Extraction completed")
    #print(tpt_jobs)
    end=time.time()
    print(end-start)
