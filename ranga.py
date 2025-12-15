import datetime
import time
from cloud_utils import s3upload
import snowflake.connector
print(datetime.datetime.now())


#2025-02-01 00:26:03.633
#2025-02-01 08:21:28.276363


#s3upload('s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/','')


import teradatasql
import pandas as pd
import datetime

print("madhava")

def getcolumninfo():
    tdcon=teradatasql.connect(
        user='demo_user',
        password='Govindagovinda@9',
        host='dev-o8ws24dp4xzqbj8b.env.clearscape.teradata.com')
    print("Venkateshwara")
    cur=tdcon.cursor()
    sql="""SELECT PS_PARTKEY,PS_SUPPKEY,PS_AVAILQTY,PS_SUPPLYCOST,LOAD_DTTM,UPDATE_DTTM,EFFSTART_DTTM,EFFEND_DTTM FROM DEMO_USER.IOP WHERE (LOAD_DTTM > CAST('1900-01-01 21:59:55.124' AS TIMESTAMP) or UPDATE_DTTM < CAST('None' AS TIMESTAMP));"""
    #sql="SELECT 'RANGA';"
    cur.execute(f"{sql}")
    result=cur.fetchall()
    #print(sql)
    print(result)
    return result
    #return [databasename,tablename]

#print(getcolumninfo())

curr_datetime = str(datetime.datetime.now())[:16]
print(curr_datetime)

print(datetime.datetime.now())
