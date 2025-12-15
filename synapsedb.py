import pyodbc
'''
# Connection details
server = 'azuresynapsetoblob.sql.azuresynapse.net'
database = 'testsqlpool'
username = 'sqladminuser'
password = 'Govindagovinda@9'
driver = '{ODBC Driver 17 for SQL Server}'
print("Ranga")
# Establish a connection
print(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')

# Create a cursor object
cursor = conn.cursor()
print("GOVINDA")
# Execute a query
query = 'SELECT TOP 10 * FROM Salesreq.Customers'
cursor.execute(query)


print(cursor)
# Fetch and print the results
rows = cursor.fetchall()
for row in rows:
    print(row)

# Close the connection
conn.close()

'''
import pandas as pd
from sqlalchemy import create_engine

# Connection details


server = 'azuresynapsetoblob.sql.azuresynapse.net'
database = 'testsqlpool'
username = 'sqladminuser'
password = 'Govindagovinda@9'
driver = 'ODBC Driver 17 for SQL Server'
'''
# Create a connection string
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&timeout=30'
print("RANGA")
# Create an SQLAlchemy engine
engine = create_engine(connection_string)
'''


conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"

# Connect and run query
with pyodbc.connect(conn_str) as conn:
    query = "SELECT  col_datetime2, col_datetime, col_smalldatetime, col_date, col_time, col_float, col_real, col_decimal, col_numeric, col_money, col_smallmoney, col_bigint, col_int, col_smallint, col_tinyint, col_bit, col_nvarchar, col_nchar, col_varchar, col_char, col_varbinary, col_binary FROM Salesreq.testall"
    df = pd.read_sql(query, conn)
# Write your SQL query
query = 'SELECT TOP 10 * FROM Salesreq.testall'

# Use pandas.read_sql to execute the query and load the data into a DataFrame
#df = pd.read_sql(query, engine)

# Print the DataFrame
print(df)

