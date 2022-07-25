# Amazon EMR Serverless and Amazon MSK Serverless Demo

Source code for the upcoming blog post, "Serverless Analytics on AWS: Getting Started with Amazon EMR Serverless and MSK Serverless". Using the newly released Amazon EMR Serverless and Amazon MSK Serverless for batch and streaming analytics with Apache Spark and Apache Kafka.

# Spark Jobs

* `01_example_console.py`: Batch read from Kafka topic, aggregation to console
* `02_example_csv_s3.py`: Batch read from Kafka topic, aggregation to CSV in S3
* `03_example_kafka.py`: Batch read from Kafka topic, aggregation to Kafka topic
* `04_stream_sales_to_kafka.py`: Streaming writes to Kafka topic
* `05_streaming_kafka.py`: Streaming reads from Kafka topic, aggregations over sliding event-time window to console


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

# Submit Spark Job to Amazon EMR Serverless using AWS CLI

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

---
The contents of this repository represent my viewpoints and not of my past or current employers, including Amazon Web Services (AWS). All third-party libraries, modules, plugins, and SDKs are the property of their respective owners.