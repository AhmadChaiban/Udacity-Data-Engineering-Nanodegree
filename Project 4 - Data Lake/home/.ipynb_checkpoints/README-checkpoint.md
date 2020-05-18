# Project: Data Lakes with Spark

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Sparkify

Sparkify require analytics, maybe even offer recommendations to their clients, or other uses cases. In order to do so, they would require that their data be processed and ready. Perhaps at some point, their database has grown past a certain point where they need a data lake. It would then be necessary to use tools like Spark and storage systems like HDFS to better deal with their data.

## Files: 

1. **create_s3.py** Creates an s3 bucket using boto3.
2. **etl.py** Runs the ETL process required to get the songs, users, artists, time and songplays tables.
3. **delete_s3.py** Empties and deletes the s3 bucket.
4. **unzip_files.py** unzips the zip files (a necessary step to upload to s3).
5. **Notebook.ipynb** is where some preliminiary data wrangling was done. 
6. **dl.cfg** Contains the AWS secret and ID 

## ETL Design

In this project, I have created my own S3 (I felt like I could use the practice with boto3), where I process the data in Spark and write it back to the S3. I have left a small figure for this design:

![ETL](ETL_Structure.PNG)

## Star Schema 

Below is a diagram of the Star Schema for this project

![schema](star_schema.PNG)