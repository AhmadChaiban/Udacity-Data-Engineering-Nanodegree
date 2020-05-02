# Project: Data Warehouse
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## File details 
The files in this repo should be run in the following order: 

1. **/RedshiftCluster/create_cluster.py** (Creates the redshift cluster)
2. **./create_tables.py** (creates the necessary tables)
3. **./etl.py**  (Copies the data and inserts it into the star schema)
4. **./analytical_queries.py** (runs some queries on the tabes created)
4. **./delete_cluster.py** (deletes the redshift cluster)

Additional files:

1. **./RedshiftCluster/cluster_status.py** (checks the cluster status)
2. **./sql_queries.py** (Houses all the queries the create, copy, drop, insert and count)