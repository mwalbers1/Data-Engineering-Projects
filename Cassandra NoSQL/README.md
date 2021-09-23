# Cassandra NoSQL ETL Project

## Overview
A music company called **Sparkify** collected song information and user activity data on their streaming app. Currently, there is no easy way to query the data to generate the results, since the data resides in a directory of CSV files on user activity on the app.  The goal for this project is to build an ETL pipeline to create an Apache Cassandra database which can generate queries on song play data to answer business questions.

## Datasets
For this project, a data set consists of a directory of CSV files representing event data partitioned by date. Two examples are:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## ETL Pipeline

1. Iterate through all events CSV files in the /event_data folder 

2. Combine each individual event CSV file into a single CSV file called `event_datafile_new.csv`

## Cassandra Data Modeling

Pieces of the primary key are labeled as “partition” or “clustering”.

### Scenario 1

PRIMARY KEY (col1, col2)

The partition key is the first field in the primary key.  The clustering key/keys are the subsequent fields in the primary key. In this example, col2 is the clustering key.


### Scenario 2

PRIMARY KEY ((col1, col2), col3)

Here the primary key is a "compound primary key" consisting of col1 and col2.  The col3 is the clustering key.
