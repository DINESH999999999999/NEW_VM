print("Kasava")

d='dinesh.csv'

d=d.replace('.csv','')
print(d)

import datetime
import pytz
import teradatasql
# Get the current time in UTC
now = datetime.datetime.now()
print(now)

def tdquery(query):
    tdcon=teradatasql.connect(
        user='demo_user',
        password='Govindagovinda@9',
        host='dev-o8ws24dp4xzqbj8b.env.clearscape.teradata.com')
    print("Venkateshwara")
    cur=tdcon.cursor()
    sd=cur.execute(query)
    result=sd.fetchall()
    print(result)
    return result


print("TERADATA",tdquery("SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26))"))
print("PYTHON",datetime.datetime.now())

now = datetime.datetime.now(pytz.utc)
print(now)