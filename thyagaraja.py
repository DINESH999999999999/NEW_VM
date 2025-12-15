import snowflake.snowpark as snowpark
from snowflake.snowpark import Session


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
print(configtable)