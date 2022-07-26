# Amazon EMR Serverless/MSK Serverless Demo

Source code for the blog post, [Serverless Analytics on AWS: Getting Started with Amazon EMR Serverless and MSK
Serverless](https://garystafford.medium.com/serverless-analytics-on-aws-getting-started-with-amazon-emr-serverless-and-amazon-msk-serverless-67155fa0f5e0)
. Using the newly released Amazon EMR Serverless and Amazon MSK Serverless for batch and streaming analytics
with Apache Spark and Apache Kafka.

## Spark Jobs

* `01_example_console.py`: Batch read from Kafka topic, aggregation to console
* `02_example_csv_s3.py`: Batch read from Kafka topic, aggregation to CSV in S3
* `03_example_kafka.py`: Batch read from Kafka topic, aggregation to Kafka topic
* `04_stream_sales_to_kafka.py`: Streaming writes to Kafka topic
* `05_streaming_kafka.py`: Streaming reads from Kafka topic, aggregations over sliding event-time window to console

## Spark Job JAR Dependencies

Upload to S3 in advance.

```shell
# dependencies
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.4/aws-msk-iam-auth-1.1.4-all.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming_2.12/3.2.1/spark-streaming_2.12-3.2.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-tags_2.12/3.2.1/spark-tags_2.12-3.2.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.2.1/spark-token-provider-kafka-0-10_2.12-3.2.1.jar

# optional dependencies for avro-format messages
wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.2.1/spark-avro_2.12-3.2.1.jar
```

## Create Kafka Topics and Messages from Client EC2 Instance

Example commands.

```shell
cd kafka_2.12-2.8.1

# *** CHANGE ME ***
export BOOTSTRAP_SERVERS=<your_bootstrap_server>:9098

# create topics 3x: topicA, topicB, topicC
bin/kafka-topics.sh --create --topic topicA \
    --partitions 6 \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --command-config config/client.properties

# list topics to confirm creation
bin/kafka-topics.sh --list \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --command-config config/client.properties

# write messages to topic
bin/kafka-console-producer.sh \
    --topic topicA \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --producer.config config/client.properties

# copy and paste contents of 'sample_data/sales_messages.txt' and then Ctrl+C to exit producer

# display messages in topics
bin/kafka-console-consumer.sh \
    --topic topicA \
    --from-beginning --max-messages 10 \
    --property print.value=true \
    --property print.offset=true \
    --property print.partition=true \
    --property print.timestamp=true \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --consumer.config config/client.properties
```

## Submit Spark Job to Amazon EMR Serverless using AWS CLI

Example commands.

```shell
aws emr-serverless start-job-run \
    --application-id <your_application_id> \
    --execution-role-arn <your_execution_role_arn> \
    --name 01-example-console \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<your_s3_bucket>/scripts/01_example_console.py",
            "sparkSubmitParameters": "--conf spark.jars=s3://<your_s3_bucket>/jars/*.jar"
        }
    }'

# run 04 and 05 simultaneously 
aws emr-serverless start-job-run \
    --application-id <your_application_id> \
    --execution-role-arn <your_execution_role_arn> \
    --name 04-stream-sales-to-kafka \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<your_s3_bucket>/scripts/04_stream_sales_to_kafka.py",
            "sparkSubmitParameters": "--conf spark.jars=s3://<your_s3_bucket>/jars/*.jar"
        }
    }'

aws emr-serverless start-job-run \
    --application-id <your_application_id> \
    --execution-role-arn <your_execution_role_arn> \
    --name 05-streaming-kafka \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<your_s3_bucket>/scripts/05_streaming_kafka.py",
            "sparkSubmitParameters": "--conf spark.jars=s3://<your_s3_bucket>/jars/*.jar"
        }
    }'
```

## Cleaning up

Don't forget to clean up your resources when you're done with the demonstration.

```shell
# delete applicatiom, cluster, and ec2 client
aws kafka delete-cluster --cluster-arn <your_msk_serverless_cluster_arn>
aws emr-serverless delete-application --application-id <your_application_id>
aws ec2 terminate-instances --instance-ids <your_ec2_instance_id>

# all objects (including all object versions and delete markers) in the bucket 
# must be deleted before the bucket itself can be deleted.
aws s3api delete-bucket --bucket <your_s3_bucket>
```

---
The contents of this repository represent my viewpoints and not of my past or current employers, including Amazon Web
Services (AWS). All third-party libraries, modules, plugins, and SDKs are the property of their respective owners.