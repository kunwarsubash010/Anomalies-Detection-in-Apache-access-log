# Anomalies-Detection-in-Apache-access-log

#Project Overview
This project involves developing a Spark application to analyze Apache Access logs and detect anomalies in API logs. The application will extract and analyze information from semi-structured parquet logs to identify trends and anomalies based on response codes, traffic, frequent visitors, top endpoints, and content size statistics. The ultimate goal is to detect unusual patterns and potential issues within the API logs.

Features and Use Cases

The application includes the following features:

Content Size Analysis:

Calculate statistics such as minimum, maximum, and count of content size.
Identify top endpoints transferring the maximum content.
Analyze daily visited content size.
Response Code Analysis:

Analyze response codes to identify trends and anomalies.
Identify IP addresses accessing the server more than 10 times.
Analyze bad requests and extract the top 10 latest 404 requests with their endpoints and time.
Traffic Analysis:

Identify top endpoints based on traffic.
Determine frequent visitors to the server.
