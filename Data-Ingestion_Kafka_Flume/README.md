## Data Ingestion Kafka to Flume

### Setup Apache Flume to ingest log data from Apache Kafka

![](images/Apache%20Kafka%20to%20Flume.png)

### 1. Create Kafka Topic

```bash
$ kafka-topics --create --zookeeper ip-10-0-21-131.ec2.internal:2181 --replication-factor 1 --partitions 1 --topic log_data
```

<br/>**Command Line**

![](images/kafka_topic_output_1.png)

![](images/kafka_topic_output_2.png)


### 2. Run Flume Agent

```bash
$ flume-ng agent --conf conf --conf-file flume_kafka.conf --name kafka-flume
```

<br/>**Command Line**

![](images/flume_agent_output_1.png)

![](images/flume_agent_output_2.png)


### 3. Run Kafka Producer

```bash
$ kafka-console-producer --broker-list ip-10-0-31-149.ec2.internal:9092 --topic log_data < /mnt/home/maew711gmail/practice_projects/3_error.log
```

<br/>**Command Line**

![](images/run_kafka_producer_output_1.png)

![](images/run_kafka_producer_output_2.png)


### 4. View Terminal Console output from Flume command

![](images/flume_agent_output_3.png)


### 5. Flume files created in sink folder

![](images/flume_sink_folder.png)

