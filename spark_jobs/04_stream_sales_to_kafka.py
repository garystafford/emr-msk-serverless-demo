# Purpose: Amazon EMR Serverless and Amazon MSK Serverless Demo
#          Write messages from a CSV file to Kafka topicC
#          to simulate real-time streaming sales data
# Author:  Gary A. Stafford
# Date: 2022-07-27
# Note: Requires --bootstrap_servers and --s3_bucket arguments

import argparse
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType


def main():
    args = parse_args()

    spark = SparkSession \
        .builder \
        .appName("04-stream-sales-to-kafka") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    schema = StructType([
        StructField("payment_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", FloatType(), False),
        StructField("payment_date", StringType(), False),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("country", StringType(), False),
    ])

    df_sales = read_from_csv(spark, schema, args)
    df_sales.cache()

    write_to_kafka(spark, df_sales, args)


def read_from_csv(spark, schema, args):
    df_sales = spark.read \
        .csv(path=f"s3a://{args.s3_bucket}/sample_data/{args.sample_data_file}",
             schema=schema, header=True, sep="|")

    return df_sales


def write_to_kafka(spark, df_sales, args):
    options_write = {
        "kafka.bootstrap.servers":
            args.bootstrap_servers,
        "topic":
            args.write_topic,
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }

    sales_count = df_sales.count()

    for r in range(0, sales_count):
        row = df_sales.collect()[r]
        df_message = spark.createDataFrame([row], df_sales.schema)

        df_message = df_message \
            .drop("payment_date") \
            .withColumn("payment_date", F.current_timestamp())

        df_message \
            .selectExpr("CAST(payment_id AS STRING) AS key",
                        "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .options(**options_write) \
            .save()

        df_message.show(1)

        time.sleep(args.message_delay)


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description="Arguments required for script.")
    parser.add_argument("--bootstrap_servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--s3_bucket", required=True, help="Amazon S3 bucket")
    parser.add_argument("--write_topic", default="topicC", required=False, help="Kafka topic to write to")
    parser.add_argument("--sample_data_file", default="sales_incremental_large.csv", required=False, help="data file")
    parser.add_argument("--message_delay", default=0.5, required=False, help="message publishing delay")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
