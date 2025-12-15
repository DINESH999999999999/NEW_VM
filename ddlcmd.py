from sf_utils import sfquery

def create_table(sfdatabasename,sfschemaname,sftablename,uploadfoldername):
    print("JANAKIRAMA",sfdatabasename,sfschemaname,sftablename,uploadfoldername)
    query=f"""DELETE FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"""
    try:
        result=str(sfquery(query))
        returncode=0
        
    except Exception as e:
        returncode=1
        result=str(e)
    
    return [returncode,result]

