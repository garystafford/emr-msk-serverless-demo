# Purpose: Amazon EMR Serverless and Amazon MSK Serverless Demo
#          Reads stream of messages from Kafka topicC and
#          writes stream of aggregations over sliding event-time window to console (top 10 only)
# References: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
# Author:  Gary A. Stafford
# Date: 2022-07-27
# Note: Requires "--bootstrap_servers" arguments

import argparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType, TimestampType


def main():
    args = parse_args()

    spark = SparkSession \
        .builder \
        .appName("05-streaming-kafka") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    df_sales = read_from_kafka(spark, args)

    summarize_sales(df_sales)


def read_from_kafka(spark, args):
    options_read = {
        "kafka.bootstrap.servers":
            args.bootstrap_servers,
        "subscribe":
            args.read_topic,
        "startingOffsets":
            "earliest",
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }

    df_sales = spark \
        .readStream \
        .format("kafka") \
        .options(**options_read) \
        .load()

    return df_sales


def summarize_sales(df_sales):
    schema = StructType([
        StructField("payment_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", FloatType(), False),
        StructField("payment_date", TimestampType(), False),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("country", StringType(), False),
    ])

    ds_sales = df_sales \
        .selectExpr("CAST(value AS STRING)", "timestamp") \
        .select(F.from_json("value", schema=schema).alias("data"), "timestamp") \
        .select("data.*", "timestamp") \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy("country",
                 F.window("timestamp", "10 minutes", "5 minutes")) \
        .agg(F.sum("amount"), F.count("amount")) \
        .orderBy(F.col("window").desc(),
                 F.col("sum(amount)").desc()) \
        .select("country",
                F.format_number("sum(amount)", 2).alias("sales"),
                F.format_number("count(amount)", 0).alias("orders"),
                "window.start", "window.end") \
        .coalesce(1) \
        .writeStream \
        .queryName("streaming_to_console") \
        .trigger(processingTime="1 minute") \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 10) \
        .option("truncate", False) \
        .start()

    ds_sales.awaitTermination()


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description="Arguments required for script.")
    parser.add_argument("--bootstrap_servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--read_topic", default="topicC", required=False, help="Kafka topic to read from")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
