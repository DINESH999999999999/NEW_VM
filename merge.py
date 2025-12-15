import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from sf_utils import sfquery




def mergecommand(job,export_start_time):
    print("RAGAVA")
    print(job,export_start_time)

    sfdatabasename=job[2]
    sfschemaname=job[3]
    sftablename=job[4]
    filter=job[11]
    scd_type=job[6]
    load_type=job[7]
    primarykey=list(job[9].split(","))
    print(primarykey)

    if load_type=='N':
        print(load_type)
        
        delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
        insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
        print(delstatement)
        print(insstatement)
        delreturn=sfquery(delstatement)
        insreturn=sfquery(insstatement)

    elif load_type=='F':
         
        delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename} WHERE {filter};"
        insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
        print(delstatement)
        print(insstatement)
        delreturn=sfquery(delstatement)
        insreturn=sfquery(insstatement)

    elif load_type=='I':
        
        if scd_type==0:
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            insreturn=sfquery(insstatement)
        
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
            merreturn=sfquery(merstatement)

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
            merreturn=sfquery(merstatement)


if __name__ == "__main__":
    print("SriRama")


    spcon = {
    "account": "qk97382.ap-southeast-1",
    "user": "DINESHM",
    "password": "Govindagovinda@9",
    "warehouse": "COMPUTE_WH",
    "database": "DATAMIGRATION"
    }

    query="""SELECT * FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE WHERE 'I'='I';"""
    
    spsession=Session.builder.configs(spcon).create()
    test=spsession.sql(query)
    #print("Sridhara")
    
    #print(list(test.collect()))
    config=test.collect()
    configtable=list(config)
    job=configtable[0]
    mergecommand(job,'2025-02-04 20:07:40.638')