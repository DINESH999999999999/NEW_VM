import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Token
from sqlparse.tokens import DML,Keyword

query = r"""SELECT C1, 
                  CAST(A.PS_PARTKEY AS VARCHAR(64000)) AS C2,
                  COALESCE(CAST(B.PS_SUPPKEY AS VARCHAR(64000)), 'GOVINDA') AS VARADHA,
                  CAST(A.PS_AVAILQTY AS VARCHAR(64000)) AS din,
                  CAST(B.PS_SUPPLYCOST AS VARCHAR(64000)) AS dd,
                  SUBSTR(CAST(A.LOAD_DTTM AS VARCHAR(64000)),2) AS GOV
           FROM SERVICE A 
           JOIN SERVICE B ON A.LOAD_DTTM = B.LOAD_DTTM"""

query = r"CAST(A.PS_PARTKEY AS VARCHAR(64000)) AS C2"
parsed = sqlparse.parse(query)[0]
columns = []

for token in parsed.tokens:
    print("GoVINDA")
    print(token)
print(columns)
