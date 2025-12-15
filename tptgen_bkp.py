def tpt_script_generator(job):
    #for job in configtable:
    print("Govinda")
    #print(job)
    tddbname=job[0]
    tdtablename=job[1]
    scdtype=job[6]
    loadtype=job[7]
    cdc=job[8]
    delimiter=job[10]
    filterconditon=job[11]
    trim=job[12]
    encrpt=job[13]
    curr_datetime = str(datetime.datetime.now())[:16]
    curr_datetime=curr_datetime.replace(" ","_")
    curr_datetime=curr_datetime.replace(":","")
    curr_datetime=curr_datetime.replace("-","")
    #loadtype='I'
    #cdc='LOAD_DTTM,UPDATE_DTTM,START_DTTM,END_DTTM'
    #scdtype=2

    tptexpdir=r'/media/ssd/exportfiles'
    tptjobname=tddbname+"_"+tdtablename+"_TPT_JOB"
    exportfilename=tddbname+"_"+tdtablename+"_TPT_"+curr_datetime+".csv"
    schemaname=f"TPT_SCH_{tdtablename}"
    selsmnt=""
    
    print(type(encrpt))
    print(tddbname,tdtablename,scdtype,loadtype,cdc,delimiter,filterconditon,trim,encrpt)
    #print(tpt_jobs)
    
    collist=getcolumninfo(tddbname,tdtablename)
    
    
    for col in collist:
        selsmnt=selsmnt+col[3]+","    
    
    selsmnt="SELECT "+selsmnt
    selsmnt=selsmnt[:-1]
    
    print(selsmnt)
    print(type(collist))
    
    condition=""
    
    if loadtype=='N':
        condition="(1=1)"
    
    if loadtype=='F':
        condition="("+filterconditon+")"
    
    if loadtype=='I':
        if scdtype==0:
            #auditcondition=getcolumninfo
            condition=f"({cdc}>'2024-12-25 23:08:45')"
            
        if scdtype==1:
            condition="("
            clms=cdc.split(",")
            for clmnitem in clms:
                condition=condition+f"{clmnitem} > '2024-12-25 23:08:45' or "
            condition=condition[:-4]+")"
        
        if scdtype==2:
            condition="("
            clms=cdc.split(",")
            for clmnitem in clms:
                condition=condition+f"{clmnitem} > '2024-12-25 23:08:45' or "
            condition=condition[:-4]+")"
    
    print(selsmnt)
    print(condition)
    
    tpt_extract_query=selsmnt + f" FROM {tddbname}.{tdtablename} WHERE "+condition+";"
    
    print(tptjobname)
    print(exportfilename)        
    print(tpt_extract_query)
    
    tptfilename=fr"/media/ssd/tptscripts/{tptjobname}.tpt"
    
    print(tptfilename)
    #tpt_jobs.append([tptfilename,exportfilename,job])
    
    with open(tptfilename, "w") as w:
    ###USING CHARACTER SET UTF8
        sql = f"""
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
                colstr = "      "+commastr + col[3] + " " + "VARCHAR(10)"+ " \n"
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
        VARCHAR TdpId = 'dev-qdoalers1wc39506.env.clearscape.teradata.com',
        VARCHAR UserName = 'demo_user',
        VARCHAR UserPassword = 'Dineshdinesh@9',
        VARCHAR SelectStmt = '{tpt_extract_query}'                   
        );             
        /*****************************/ 
        
        /*****************************/
        
        APPLY TO OPERATOR (FILE_WRITER_OPERATOR[2])
        SELECT * FROM OPERATOR (EXPORT_OPERATOR[2]);
            ); 
        /*****************************/
"""
        w.write(sql + " \n")
    return [tptfilename,exportfilename]
        