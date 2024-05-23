#Importing necessary libaries
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os 
import re

# Initialize Spark session
spark = SparkSession.builder.appName("Apache Log Analysis").getOrCreate()

# Define log file path
log_file_path = '/Users/shubhash/Desktop/Project 1/Anomalies-Detection-in-Apache-access-log/access.log'

# Checking if the log file exists
if os.path.exists(log_file_path):
    print("Log File exists. Proceeding to reading this file. ")
else:
    print("Log File not Found. Please check the file path. ")
    exit(1)

## Function to parse log entries
def parse_log_entry(entry):
    pattern = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\d+) "([^"]*)" "([^"]*)" "([^"]*)"'
    match = re.match(pattern, entry)

    if match:
        ip_address = match.group(1)
        _dash = match.group(2)
        _user = match.group(3)
        timestamp = match.group(4)
        method = match.group(5)
        endpoint = match.group(6)
        http_version = match.group(7)
        response_code = int(match.group(8))
        content_size = int(match.group(9))
        referrer = match.group(10)
        user_agent = match.group(11)
        _extra = match.group(12)

        return Row(
            ip_address=ip_address,
            timestamp=timestamp,
            method=method,
            endpoint=endpoint,
            http_version=http_version,
            response_code=response_code,
            content_size=content_size,
            referrer=referrer,
            user_agent=user_agent
        )
    else:
        return None

## Read log file into DataFrame
logs_df = spark.read.text(log_file_path)

# Parse log entries and create DataFrame
parsed_logs_df = logs_df.rdd.map(lambda row:parse_log_entry(row.value)).filter(lambda x: x is not None).toDF()

# Analyze response codes
response_code_counts = parsed_logs_df.groupBy('response_code').count().orderBy('count', ascending=False)
response_code_counts.show(10)

# Identifying IP addresses accessing the server more than 10 times
ip_counts = parsed_logs_df.groupBy('ip_address').count().orderBy('count', ascending= False)
frequent_ips = ip_counts.filter(ip_counts['count'] > 100)
frequent_ips.show()

# Stoping the Spark session
spark.stop()