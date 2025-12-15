import pyspark
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[*]") \
                    .appName('BigData-ETL.com') \
                    .getOrCreate()


print(spark)
print(spark.version)    
print("NARAYANA")

print("NARAYANA")