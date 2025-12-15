import re

query = r"""
SELECT C1 AS vss, concat(A.PS_SUPPKEY,B.PS_SUPPKEY), CAST(A.PS_PARTKEY AS VARCHAR(64000))      AS   C2, concat(A.PS_SUPPKEY,B.PS_SUPPKEY) AS C4, 
       COALESCE(CAST(B.PS_SUPPKEY AS VARCHAR(64000)), 'GOVINDA') AS VARADHA, 
      CAST('A.PS_AVAILQTY' AS VARCHAR(64000)) AS din,CAST(B.PS_SUPPLYCOST AS VARCHAR(64000))   AS     dd   , 
      SUBSTR(CAST(A.LOAD_DTTM AS VARCHAR(64000)),2) AS GOV,C11,C1 
FROM SERVICE A 
JOIN SERVICE B ON A.LOAD_DTTM = B.LOAD_DTTM
"""

# Extract the SELECT part of the query
select_part = re.search(r'SELECT(.*?)FROM', query, re.S).group(1)

# Split the SELECT part into individual column expressions
columns = re.split(r',\s*(?![^()]*\))', select_part.strip())

# Print each column expression
for column in columns:
    #print(column)
    column=column.replace("\t"," ")
    column=column.replace("\n"," ")
    alias_chk_list = column.split(" ")
    while '' in alias_chk_list:
        alias_chk_list.remove('')
    
    print(column)
    if len(alias_chk_list)>2 and alias_chk_list[-2] == 'AS':
        #print(alias_chk_list[-1])
        
        column = column[:column.rindex('AS')]
    print(column)
        
    