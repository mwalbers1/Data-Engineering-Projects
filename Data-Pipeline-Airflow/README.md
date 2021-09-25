
# Airflow Data Pipeline

An Airflow DAG (Directed Acyclic Graph) consists of several tasks for the loading of S3 song and events data into an AWS Redshift database.  The S3 songs and events data are in JSON format.

![](images/udacity_project_dag.png)

## Overview

The project consists of the following files.

/home/workspace folder

- dwh.cfg
- add_redshift_connection.sh
- add_aws_connection.sh
- project-dwh-redshift.py

/home/workspace/airflow/dags folder

- udacity_project_dag.py
- staging_subdag.py

### Setup Instructions

1. Update dwh.cfg with AWS access id and secret key

2. Create AWS Redshift Cluster server by running "python project-dwh-redsift.py" at terminal

3. Start Airflow server by running "/opt/airflow/start.sh" at terminal

4. Add redshift connection to Airflow by running add_redshift_connection.sh at terminal

5. Run add_aws_connection.sh at terminal to create Airflow aws_credentials connection

6. Execute create_tables.sql on newly created Redshift Cluster database

### Load Staging Tables

There are two load staging table tasks, one for the songs staging data and the other for events data.  Each load staging
task calls an Airflow subdag. The subdag file is called staging_subdag.py. The staging subdag is responsible for drop/create of the staging table, loading Json data into staging table, and then validating that data was loaded into staging table.

### Load Fact Table

The fact table called songplays is loaded from an Airflow task which is dependent on both load staging tasks

### Load Dimension Tables

Each of the four Dimension tables run in separate tasks in parallel.  All four dimension table tasks are dependent on the fact table load task.

### Final data validation tasks

A final task runs data validation checks on the dimension and fact table(s)

## Summary

The two staging table tasks were designed to call a subdag which encapsulates the generic logic for loading a staging table such as dropping and recreating the staging table, loading the staging table via the Redshift COPY command and a data validation task. 

The final data validation task was added to the staging subdag so that we can prevent potential issues with the staging data from flowing downstream into the dimension and fact tables.
