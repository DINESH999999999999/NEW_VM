import pandas as pd

# Replace 'input.csv' with the path to your CSV file
csv_file_path = '/media/ssd/exportfiles/DEMO_USER_SERVICE_NOW_TPT_20250315_1143-1-1.csv'
# Replace 'output.parquet' with the desired path for the Parquet file
parquet_file_path = '/media/ssd/service_now.parquet'

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path,header=None)

print(df.count())



df.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'LOAD_DTTM', 'UPDATE_DTTM', 'EFFSTART_DTTM', 'EFFEND_DTTM', 'BYTEINT_COL', 'SMALLINT_COL', 'BIGINT_COL', 'INTEGER_COL', 'DECIMAL_COL', 'FLOAT_COL', 'NUMBER_COL', 'CHAR_COL', 'VARCHAR_COL', 'DOB_COL', 'DOB1_COL', 'DOB2_COL', 'START_TIME', 'END_TIME', 'TIME1', 'TS_TZ'
]

print(df.columns)

# Write the DataFrame to a Parquet file
df.to_parquet(parquet_file_path, engine='pyarrow')

df.to_csv('/media/ssd/service_now.csv')

print(f'CSV file has been successfully converted to Parquet and saved as {parquet_file_path}')

