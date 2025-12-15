import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        return encoding

file_path = '/media/ssd/exportfiles/DEMO_USER_SERVICE_TPT_20250305_0139-1-1.csv'
file_path = '/media/ssd/python/part-00002-bacff64f-8fde-4c82-ae76-456b227cadbf-c000.snappy.parquet'
encoding = detect_encoding(file_path)
print(f"The file encoding is: {encoding}")
