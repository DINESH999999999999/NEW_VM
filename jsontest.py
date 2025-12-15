import json
import snowflake.connector

with open('/media/ssd/python/credentials.json','r+') as config_file:
    cred=json.load(config_file)
    print(cred['td_host'])
    print(
	cred['td_host'           ],
	cred['td_user'           ],
	cred['td_password'       ],
	cred['sf_host'           ],
	cred['sf_user'           ],
	cred['sf_password'       ],
	cred['sf_warehouse'      ],
	cred['sf_database'       ],
	cred['sf_schema'         ],
	cred['tpt_script_path'   ],
	cred['tpt_export_path'   ],
	cred['job_log_path'      ],
	cred['tpt_instance_count'])


td_host = cred['td_host']
td_user = cred['td_user']
td_password = cred['td_password']
sf_host = cred['sf_host']
sf_user = cred['sf_user']
sf_password = cred['sf_password']
sf_warehouse = cred['sf_warehouse']
sf_database = cred['sf_database']
sf_schema = cred['sf_schema']
tpt_script_path = cred['tpt_script_path']
tpt_export_path = cred['tpt_export_path']
job_log_path = cred['job_log_path']
tpt_instance_count = cred['tpt_instance_count']

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
"database": sf_database,
"schema":sf_schema,
"warehouse":sf_warehouse
}



print(sfcon)
print(spcon)