import subprocess
import os
import glob
import json
from sf_utils import sfquery
from tpt_utils import tdquery
from sf_utils import getcdcdates
import teradatasql
print("Padmanabha")



#s3upload('s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/','DEMO_USER_QWE_TPT_20250208_2235')

def azupload(az_path,filename):

    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)
    
    tpt_export_path = cred['tpt_export_path']

    azcopy_app = r'/media/ssd/azcopy_linux_amd64_10.28.0/azcopy'
    sas_token = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2125-03-02T17:05:08Z&st=2025-03-02T09:05:08Z&spr=https&sig=n7954awhyJo9LhAfYNNY3SF74A1xk77YD8ZpCjlhwbc%3D"

    cmd=f"""{azcopy_app} cp '{tpt_export_path}/*{filename}*' '{az_path}{filename}/{sas_token}' --recursive """
    print("krisha")
    print(cmd)
    t=subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(t.returncode)
    print(t.stdout)
    # 
    
    if t.returncode==0:
        out_log=t.stdout
        print("RAMA")
        out_log=out_log.split("\n")
        log_file_name=""
        for i in range(0,len(out_log)):
            if 'Log file is located at:' in out_log[i]:
                print(out_log[i])
                log_file=out_log[i].strip()
                log_file_name=log_file[log_file.index('at:')+4:]
                break

        with open(rf'{log_file_name}') as lf:
            log_content=str(lf.read())
            print(log_content)
        
        log_content_list=[]
        log_content_list=log_content.split("\n")
        
        uploaded_csv_files=''
        csv_cnt=0
        for i in range(0,len(log_content_list)):
            print("GOVINDA")
            if 'Starting transfer: Source' in log_content_list[i]:
                file = log_content_list[i]
                csv_name = file[file.index('Source')+7:file.index('Destination')] 
                print(log_content_list[i])
                print(csv_name)
                csv_cnt=csv_cnt+1
                uploaded_csv_files = uploaded_csv_files + '\n' + csv_name
            


        csv_cnt_header="No Of File Uploaded : "+str(csv_cnt)
        print("KRISHNA")
        print(uploaded_csv_files)
        print(csv_cnt_header)
        
        final_log = csv_cnt_header + '\n' + uploaded_csv_files

        print(final_log)

        return [t.returncode,cmd,final_log]

    else:
        print("")
        print(t.stderr)
        print(t.stdout)
        return [t.returncode,cmd,t.stdout]


#print(azupload('https://snowflaketeradata213.blob.core.windows.net/teradataexport/TDEXPORT/DATAMIGRATION/DEMO_USER/','DEMO_USER_SERVICE_TPT_20250303_0536'))

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

def tdcount(tddbname,tdtablename,loadtype,custom_sql):
    #query2=f"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26));"
    #export_start_time=tdquery(query2)[0][0]
    try:
        if loadtype == 'CUSTOM_SQL':
            core_statement = custom_sql[custom_sql.index('FROM'):]
            query1=f"SELECT COUNT(*) {core_statement}"
            print(loadtype)
            print(query1)
        else:
            query1=f"SELECT COUNT(*) FROM {tddbname}.{tdtablename};"
        tdcnt=tdquery(query1)[0][0]
        returncode=0
    except Exception as e:
        returncode=1
        tdcnt=str(e)
        

    #return [returncode,tdcnt,export_start_time]
    return [returncode,tdcnt]

print(tdcount('as','as','CUSTOM_SQL',"SELECT PS_AVAILQTY , PS_SUPPKEY , PS_SUPPLYCOST , PS_AVAILQTY AS C1 , PS_SUPPKEY AS C3  FROM CUSTOMER WHERE PS_AVAILQTY=2"))


def create_table(sfdatabasename,sfschemaname,sftablename,loadtype,schcol):
    print("JANAKIRAMA",sfdatabasename,sfschemaname,sftablename,schcol)
    if loadtype == 'CUSTOM_SQL':
        query="""CREATE OR REPLACE TABLE {}.{}_WRK.{} ("""
        for i in schcol:
            c=i.strip()
            c=c.replace("\n"," ")
            c=c.replace("\t"," ")
            query= query + c 
        query = query+");"
        print(query)
    else:
        query=f"""DELETE FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"""



'''

#print(create_stage('DATAMIGRATION','DEMO_USER_WRK','s3://tdsfbucket/TDEXPORT/DATAMIGRATIONSSS/DEMO_USER/'))
import re
def getcolumninfozz(databasename,tablename,loadtype,custom_sql):
    if loadtype == 'CUSTOM_SQL':
        print(loadtype,custom_sql,databasename,tablename)
        custom_columns = []
        custom_sql = r"""
                SELECT C1 AS vss, concat(A.PS_SUPPKEY,B.PS_SUPPKEY), CAST(A.PS_PARTKEY AS VARCHAR(64000))      AS   C2, concat(A.PS_SUPPKEY,B.PS_SUPPKEY) AS C4, 
                    COALESCE(CAST(B.PS_SUPPKEY AS VARCHAR(64000)), 'GOVINDA') AS VARADHA), 
                    CAST('A.PS_AVAILQTY' AS VARCHAR(64000)) AS din,CAST(B.PS_SUPPLYCOST AS VARCHAR(64000))   AS     dd   , 
                    SUBSTR(CAST(A.LOAD_DTTM AS VARCHAR(64000)),2) AS GOV,C11,C1 
                FROM (SELECT * FROM ORDERS D) A 
                JOIN (SELECT * FROM CUSTOMER Z) B ON A.LOAD_DTTM = B.LOAD_DTTM
                """
        custom_sql = r"""
SELECT CAST(B.PS_SUPPKEY AS VARCHAR(64000)),
CAST(concat(A.PS_SUPPKEY,'HC') AS VARCHAR(64000)) AS COLUMN_2 ,
CAST(CAST(A.PS_PARTKEY AS VARCHAR(64000)) AS VARCHAR(64000)) AS COLUMN_3 ,
CAST(concat(A.PS_SUPPKEY,'JNJ') AS VARCHAR(64000)) AS COLUMN_4 

 FROM (SELECT * FROM ORDERS D) A 
                JOIN (SELECT * FROM CUSTOMER Z) B ON A.LOAD_DTTM = B.LOAD_DTTM"""
        
        custom_sql = r"""
SELECT PS_AVAILQTY , PS_SUPPKEY , PS_SUPPLYCOST , PS_AVAILQTY AS C1 , PS_SUPPKEY AS C3  FROM CUSTOMER"""
        
        select_part = re.search(r'SELECT(.*?)FROM', custom_sql, re.S).group(1)
        remaining_part = custom_sql[custom_sql.index('FROM'):]
        print(remaining_part)
        columns = re.split(r',\s*(?![^()]*\))', select_part.strip())
        col_cnt=0
        rows=[]
        sel_stmnt=""
        print(select_part)
        for column in columns:
            #print(column)
            column=column.replace("\t"," ")
            column=column.replace("\n"," ")
            alias_chk_list = column.split(" ")
            col_cnt=col_cnt+1
            while '' in alias_chk_list:
                alias_chk_list.remove('')
            
            
            print("Rama", column)
            teradata_data_types = [
                "BYTEINT", "SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE PRECISION",
                "CHAR", "CHARACTER", "VARCHAR", "CHARACTER VARYING",
                "DATE", "TIME", "TIMESTAMP", "INTERVAL",
                "BYTE", "VARBYTE",
                "BLOB", "CLOB",
                "PERIOD(DATE)", "PERIOD(TIME)", "PERIOD(TIMESTAMP)"]
            
            key_exception=0

            for j in teradata_data_types:
                if j in alias_chk_list[-1]:
                    key_exception = 1
            if len(alias_chk_list)>2 and alias_chk_list[-2] == 'AS':#and key_exception==0:
                #print(alias_chk_list[-1])
                print(column)
                column = column[:column.rindex('AS')]

            print(column)
            custom_columns.append(column.strip())
            column = column.strip()
            column = f"CAST({column} AS VARCHAR(64000)) AS COLUMN_{col_cnt}"
            print(column)
            sel_stmnt = sel_stmnt + ',' + column + ' '
            rows.append([databasename,tablename,col_cnt,'COLUMN_'+str(col_cnt),'VARCHAR(64000)',column.strip(),''])
        sel_stmnt = "SELECT "+sel_stmnt[1:] +" "+ remaining_part

        print("KRISHNA")
        print(sel_stmnt)
        rows[-1][-1]=sel_stmnt
        return rows

    else:
        with open('/media/ssd/python/credentials.json','r+') as config_file:
            cred=json.load(config_file)

        td_host = cred['td_host']
        td_user = cred['td_user']
        td_password = cred['td_password']

        tdcon=teradatasql.connect(
            user=td_user,
            password=td_password,
            host=td_host)
        
        print("Venkateshwara")
        cur=tdcon.cursor()
        sql="""    
        SELECT Coalesce(Trim(DATABASENAME),''), Coalesce(Trim(TABLENAME),''), 
        TRIM(ROW_NUMBER() OVER(PARTITION BY   TABLENAME ,  DATABASENAME ORDER BY COLUMNID )), 
        Coalesce(Trim(COLUMNNAME),''), Trim(Coalesce(COLUMN_DATATYPE,''))
        ,Coalesce(Trim(ColumnLength),''),Coalesce(Trim(ColumnFormat),'') FROM
        (
        select c.tablename ,  c.DATABASENAME , c.COLUMNNAME, c.ColumnLength , c.ColumnFormat ,  CASE c.ColumnType
            WHEN 'BF' THEN 'BYTE('            || TRIM(ColumnLength (FORMAT '-(9)9')) || ')'
            WHEN 'BV' THEN 'VARBYTE('         || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
            WHEN 'CF' THEN 'CHAR('            || TRIM( CASE WHEN ColumnLength * 2 > 64000 THEN 64000 ELSE ColumnLength * 2 END (FORMAT 'Z(9)9')) || ')'
            WHEN 'CV' THEN 'VARCHAR('         || TRIM(64000 (FORMAT 'Z(9)9')) || ')'
            WHEN 'D ' THEN 'DECIMAL('         || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'DA' THEN 'INTDATE' /* DATE WAS HERE*/
            WHEN 'F ' THEN 'FLOAT'
            WHEN 'I1' THEN 'BYTEINT'
            WHEN 'I2' THEN 'SMALLINT'
            WHEN 'I8' THEN 'BIGINT'
            WHEN 'I ' THEN 'INTEGER'
            WHEN 'AT' THEN 'TIME('            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'TS' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'TZ' THEN 'TIME('            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
            WHEN 'SZ' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
            WHEN 'YR' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'YM' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MONTH'
            WHEN 'MO' THEN 'INTERVAL MONTH('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'DY' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'DH' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO HOUR'
            WHEN 'DM' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MINUTE'
            WHEN 'DS' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'HR' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'HM' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MINUTE'
            WHEN 'HS' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'MI' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'MS' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'SC' THEN 'INTERVAL SECOND(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'BO' THEN 'BLOB('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
            WHEN 'CO' THEN 'CLOB('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
        
            WHEN 'PD' THEN 'PERIOD(DATE)'
            WHEN 'PM' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
            WHEN 'PS' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
            WHEN 'PT' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
            WHEN 'PZ' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
            WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)
        
            WHEN '++' THEN 'TD_ANYTYPE'
            WHEN 'N'  THEN 'NUMBER('          || CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE TRIM(DecimalTotalDigits (FORMAT '-(9)9')) END
                                            || CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) END
                                            || ')'
            WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
            WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
        
            WHEN 'JN' THEN 'JSON('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
            WHEN 'VA' THEN 'TD_VALIST'
            WHEN 'XM' THEN 'XML'
        
            ELSE '<Unknown> ' || ColumnType
        END  COLUMN_DATATYPE ,  INDEXTYPE,
        CASE INDEXTYPE
        WHEN  'P'     then 'Nonpartitioned primary index'
        WHEN  'Q'     then 'Partitioned primary index'
        WHEN  'S'     then 'Secondary index'
        WHEN  'J'     then 'n index'
        WHEN  'N'    Then 'Hash index'
        WHEN  'K'     then 'Primary key'
        WHEN  'U'     then 'Unique constraint'
        WHEN  'V'     then 'Value-ordered secondary index'
        WHEN  'H'     then 'Hash-ordered ALL covering secondary index'
        WHEN  'O'     then 'Valued-ordered ALL covering secondary index'
        WHEN  'I'      then 'dering column of a composite secondary index'
        WHEN  'G'     then 'Geospatial non-unique secondary index.'
        when 'M'	  then 'Multi column statistics'
        when 'D'	     then 'Derived column partition statistics'
        when '1'    	then 'field1 column of a join or hash index'
        when '2'	    then ' field2 column of a join or hash index'
        END INDEX_TYPE_NAME  ,
        ColumnPosition ,IndexNumber ,
        PartitioningColumn
        , CASE
                WHEN ColumnType IN ('CV', 'CF', 'CO')
                THEN CASE CharType
                        WHEN 1 THEN ' CHARACTER SET LATIN'
                        WHEN 2 THEN ' CHARACTER SET UNICODE'
                        WHEN 3 THEN ' CHARACTER SET KANJISJIS'
                        WHEN 4 THEN ' CHARACTER SET GRAPHIC'
                        WHEN 5 THEN ' CHARACTER SET KANJI1'
                        ELSE ''
                    END
                ELSE ''
            END STRING_TYPE ,COLUMNID
        
        from DBC.columnsV  c
        left join    DBC.IndicesV  i   on   c.tablename=i.tablename AND c.DATABASENAME=i.DATABASENAME  and c.COLUMNNAME=i.COLUMNNAME
        where upper(C.tablename)=upper('{}')
        AND upper(C.DATABASENAME)=upper('{}')
        ) a;
            """.format(tablename, databasename)
        cur.execute(f"{sql}")
        result=cur.fetchall()
        #print(sql)
        return result
        #return [databasename,tablename]


def tpt_script_generator(job):
    #for job in configtable:
    try:
        with open('/media/ssd/python/credentials.json','r+') as config_file:
            cred=json.load(config_file)

        td_host = cred['td_host']
        td_user = cred['td_user']
        td_password = cred['td_password']
        tpt_script_path = cred['tpt_script_path']
        tpt_export_path = cred['tpt_export_path']
        tpt_instance_count = cred['tpt_instance_count']


        print("Govinda")
        #print(job)
        tddbname=job[0]
        tdtablename=job[1]
        scdtype=job[6]
        loadtype=job[7]
        cdccol=job[8]
        delimiter=job[10]
        filterconditon=job[11]
        custom_sql="SELECT C1,C2,C3,C4 FROM TABLE_A"
        trim=job[12]
        encrpt=job[13]
        export_start_time=tdquery("SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26))")[0][0]
        print(export_start_time)
        curr_datetime = str(export_start_time)[:16]
        curr_datetime=curr_datetime.replace(" ","_")
        curr_datetime=curr_datetime.replace(":","")
        curr_datetime=curr_datetime.replace("-","")
        #loadtype='I'
        #cdc='LOAD_DTTM,UPDATE_DTTM,START_DTTM,END_DTTM'
        #scdtype=2

        #tptexpdir=r'/media/ssd/exportfiles'
        
        tptexpdir=tpt_export_path
        tptjobname=tddbname+"_"+tdtablename+"_TPT_JOB"
        exportfilename=tddbname+"_"+tdtablename+"_TPT_"+curr_datetime+".csv"
        schemaname=f"TPT_SCH_{tdtablename}"
        selsmnt=""
        
        print(type(encrpt))
        print(tddbname,tdtablename,scdtype,loadtype,cdccol,delimiter,filterconditon,trim,encrpt)
        #print(tpt_jobs)
        
        

        collist=getcolumninfozz(tddbname,tdtablename,loadtype,custom_sql)
        
        
        for col in collist:
            selsmnt=selsmnt+col[3]+","    
        
        selsmnt="SELECT "+selsmnt
        selsmnt=selsmnt[:-1]
        
        print(selsmnt)
        print(type(collist))
        
        condition=""
         
        if loadtype=='FULL':
            condition="(1=1)"
        
        elif loadtype=='FILTER':
            filterconditon=filterconditon.replace("'","''")
            condition="("+filterconditon+")"
        
        elif loadtype=='INCREMENTAL':

            cdcdates=getcdcdates(tddbname,tdtablename)
            if len(cdcdates)==0:
                cdcdates=['1900-01-01 00:00:00.000','1900-01-01 00:00:00.000']
                print("Govinda")
            else:
                cdcdates=cdcdates[0]
                print("KRISHNA",cdcdates,type(cdcdates),cdcdates[0][1])
            if scdtype==0:
                #auditcondition=getcolumninfo
                #condition=f"({cdc}>'2024-12-25 23:08:45')"
                print("Thyagaraja",cdccol,type(cdccol))
                
                if str(cdccol)!='None' and len(cdccol)>1:
                    condition="("
                    clms=cdccol.split(",")
                    
                    for clmnitem in range(0,len(clms)):
                        condition=condition+f"""(({clms[clmnitem]} >= CAST(''{cdcdates[0]}'' AS TIMESTAMP)) and ({clms[clmnitem]} < CAST(''{export_start_time}'' AS TIMESTAMP))) OR """
                    condition=condition[:-4]+")"

                else:
                    condition='(1=1)'

            elif scdtype==1:
                #auditcondition=getcolumninfo
                #condition=f"({cdc}>'2024-12-25 23:08:45')"
                print("Thyagaraja",cdccol,type(cdccol))
                
                if str(cdccol)!='None' and len(cdccol)>1:
                    condition="("
                    clms=cdccol.split(",")
                    
                    for clmnitem in range(0,len(clms)):
                        condition=condition+f"""(({clms[clmnitem]} >= CAST(''{cdcdates[0]}'' AS TIMESTAMP)) and ({clms[clmnitem]} < CAST(''{export_start_time}'' AS TIMESTAMP))) OR """
                    condition=condition[:-4]+")"

                else:
                    condition='(1=1)'
            
            elif scdtype==2:
                #auditcondition=getcolumninfo
                #condition=f"({cdc}>'2024-12-25 23:08:45')"
                print("Thyagaraja",cdccol,type(cdccol))
                
                if str(cdccol)!='None' and len(cdccol)>1:
                    condition="("
                    clms=cdccol.split(",")
                    
                    for clmnitem in range(0,len(clms)):
                        condition=condition+f"""(({clms[clmnitem]} >= CAST(''{cdcdates[0]}'' AS TIMESTAMP)) and ({clms[clmnitem]} < CAST(''{export_start_time}'' AS TIMESTAMP))) OR """
                    condition=condition[:-4]+")"

                else:
                    condition='(1=1)'
        
        elif loadtype == 'CUSTOM_SQL':
            print(loadtype)
            collist
            selsmnt = collist[-1][-1]





        print(selsmnt)
        print(condition)
        
        
        #tpt_extract_query=selsmnt + f" FROM {tddbname}.{tdtablename} WHERE "+condition+";"
        
        if loadtype == 'CUSTOM_SQL':
            tpt_extract_query = selsmnt
            tpt_extract_query = tpt_extract_query.replace("'","''")
        else:
            tpt_extract_query=selsmnt + f" FROM {tddbname}.{tdtablename} WHERE "+condition+";"
        print(tptjobname)
        print(exportfilename)        
        print(tpt_extract_query)
        
        #tptfilename=fr"/media/ssd/tptscripts/{tptjobname}.tpt"
        
        tptfilename=fr"{tpt_script_path}/{tptjobname}.tpt"

        print(tptfilename)
        #tpt_jobs.append([tptfilename,exportfilename,job])
        
        with open(tptfilename, "w") as w:
        ###USING CHARACTER SET UTF8
            sql = f"""
            USING CHARACTER SET UTF8 
            DEFINE JOB {tptjobname}
            DESCRIPTION 'EXPORT TERADATA_SRC'
            (
            /*****************************/ """
            w.write(sql + " \n")
            sql = f"""      DEFINE SCHEMA  {schemaname}  ("""
            w.write(sql + " \n")
            commastr=""
            for col in collist:
                if col[4] in ["DATE", "INTDATE"]:
                    #colstr = "      "+commastr + col[3] + " " + "VARDATE(10) FORMATIN('YYYY-MM-DD') FORMATOUT('YYYY-MM-DD')"+ " \n"
                    #colstr = "      "+commastr + col[3] + " " + "VARCHAR(10)"+ " \n"
                    colstr = "      "+commastr + col[3] + " " + "ANSIDATE"+ " \n"
                else:
                    colstr = "      "+commastr + col[3] + " " + col[4] + " \n"
                commastr=","
                w.write(colstr)
            sql = f"""      );
            /*****************************/ 
            /*****************************/
            DEFINE OPERATOR FILE_WRITER_OPERATOR
            DESCRIPTION 'TPT DATA CONNECTOR OPERATOR'
            TYPE DATACONNECTOR CONSUMER
            SCHEMA {schemaname}
            ATTRIBUTES                  
            (                    
            VARCHAR PrivateLogName =  '{tptjobname}_log',
            VARCHAR DIRECTORYPath = '{tptexpdir}',
            VARCHAR FileName = '{exportfilename}',
            VARCHAR IndicatorMode     = 'N',    
            VARCHAR OpenMode          = 'Write',  
            VARCHAR Format            = 'Delimited', 
            VARCHAR TextDelimiter = '{delimiter}' ,
            VARCHAR FileSizeMax = '52428800',
            /*VARCHAR QuotedData = 'Optional'    */      
            VARCHAR QuotedData = 'Yes'          
            );  
            """
                
            w.write(sql + " \n")
            sql = f"""      /*****************************/ 
            DEFINE OPERATOR EXPORT_OPERATOR
            DESCRIPTION 'TPT EXPORT OPERATOR'
            TYPE EXPORT
            SCHEMA {schemaname}
            ATTRIBUTES
            (
            VARCHAR PrivateLogName    =  '{tptjobname}_log',
            INTEGER MaxSessions = 16,
            INTEGER MinSessions = 1,
            VARCHAR TdpId = '{td_host}',
            VARCHAR UserName = '{td_user}',
            VARCHAR UserPassword = '{td_password}',
            VARCHAR SelectStmt = '{tpt_extract_query}'                   
            );             
            /*****************************/ 
            
            /*****************************/
            
            APPLY TO OPERATOR (FILE_WRITER_OPERATOR[{tpt_instance_count}])
            SELECT * FROM OPERATOR (EXPORT_OPERATOR[{tpt_instance_count}]);
                ); 
            /*****************************/
            """
            w.write(sql + " \n")
        

        with open(tptfilename, 'r') as file:
            tptcontent = file.read()
        print("RANGANATHA")
        #print(tptcontent)
        print(tptfilename)
        print(type(tptcontent))
        
        print(export_start_time)
        returncode=0
        errormsg=" "


    except Exception as e:
        print("SRINIVASA")
        returncode=1
        errormsg=e
        tptfilename=""
        tptcontent=""
        exportfilename=""
        export_start_time=""
        colstr=""
    print("KRISHNA")
    #print(returncode,errormsg,tptfilename,tptcontent,exportfilename,export_start_time)
    print('KRISHNA')
    return [returncode,errormsg,tptfilename,tptcontent,exportfilename,export_start_time,colstr]
        
a=getcolumninfozz('demo_user','customer','CUSTOM_SQL',"SELECT C1,C2,C3,C4 FROM TABLE_A")
print("Srirama")
print(type(a))
print(a)


import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
    
spcon = {
"account": "qk97382.ap-southeast-1",
"user": "DINESHM",
"password": "Govindagovinda@9",
"warehouse": "COMPUTE_WH",
"database": "DATAMIGRATION"
}
#query="""SELECT * FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE;"""
query="""SELECT TD_DATABASE_NAME,TD_TABLE_NAME,SF_DATABASE_NAME,SF_SCHEMA_NAME,SF_TABLE_NAME,WAREHOUSE_NAME,SCD_TYPE,LOAD_TYPE,CDC_COLUMNS,PRIMARY_KEY,DELIMITER,FILTER_CONDITION,
        TRIM,ENCRYPTION_COLUMNS,S3_PATH,(SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.LOG_TABLE)+1,10000)) AS BATCH_ID,JOB_ID FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE WHERE SF_TABLE_NAME<>'ORDERS'"""

spsession=Session.builder.configs(spcon).create()
batch=spsession.sql(query)
#print("Sridhara")

    

#print(list(test.collect()))
config=batch.collect()
configtable=list(config)


for i in configtable:
    print(tpt_script_generator(i))


'''