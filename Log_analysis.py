from pyspark.sql import SparkSession
from pyspark.sql import Row
import os 
import re

spark = SparkSession.builder.appName("Apache Log Analysis").getOrCreate()

log_file_path = '/Users/shubhash/Desktop/Project 1/Anomalies-Detection-in-Apache-access-log/access.log'

if os.path.exists(log_file_path):
    print("Log File exists. Proceeding to reading this file. ")
else:
    print("Log File not Found. Please check the file path. ")
    exit(1)

logs_df = spark.read.text(log_file_path)

logs_df.show(n=5, truncate=False)