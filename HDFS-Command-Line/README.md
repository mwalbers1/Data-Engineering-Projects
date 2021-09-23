# HDFS Command Line
This is a practice project from the SimpliLearn Big Data Hadoop and Spark Developer certification. The goal of this project is to practice HDFS commands from the command line terminal.

## Description

Using command lines of HDFS, perform the below tasks:

* Create a directory named “Simplilearn“ in HDFS
 
* Transfer a sample text file from your local file system to HDFS directory
 
* List the files in HDFS directory
 
* Change the permissions on the file to read, write, and execute
 
* Remove the text file from HDFS directory

## Steps to Perform

Create a directory named “Simplilearn"

         hdfs dfs -mkdir Simplilearn 

Transfer a sample text file from your local file system to HDFS directory

         hdfs dfs -put /home/simplilearn_learner/test.txt Simplilearn

List the files in HDFS directory

         hdfs dfs -ls Simplilearn 

Change the permissions on the file to read, write and execute

         hdfs dfs -chmod 700 Simplilearn/test.txt  

Remove the text file from HDFS directory

        hdfs dfs -rm -r Simplilearn/test.txt 

