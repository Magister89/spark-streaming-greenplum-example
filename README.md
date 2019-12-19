## Spark Structured Streaming BatchMode Insert in Greenplum Database

Streaming inserts in Greenplum table example using Spark 2.4.4 and Greenplum Spark Connector 1.6.1
The app consume from a Kafka topic with a predefined schema, and then BatchMode inserts in a Greenplum Table using
gpfdist with 1 minute of microbatch interval. Checkpoints are saved on HDFS.

##### Build

SBT 1.3.3 and Scala 2.11.12

##### Requirements

* greenplum-spark_2.11-1.6.1.jar
* spark-sql-kafka-0-10_2.11-2.4.4.jar
* kafka-clients-0.10.0.0.jar

##### Run Example

`spark-submit --master local[*] --deploy-mode client --jars spark-sql-kafka-0-10_2.11-2.4.4.jar,kafka-clients-0.10.0.0.jar,greenplum-spark_2.11-1.6.1.jar --driver-class-path spark-sql-kafka-0-10_2.11-2.4.4.jar,kafka-clients-0.10.0.0.jar,greenplum-spark_2.11-1.6.1.jar spark-streaming-greenplum-example_2.11-1.0.0.jar`

