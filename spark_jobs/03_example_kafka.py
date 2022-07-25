# Purpose: Amazon EMR Serverless and Amazon MSK Serverless Demo
#          Reads messages from Kafka topicA and write aggregated messages to topicB
# Author:  Gary A. Stafford
# Date: 2022-07-24

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType, TimestampType
from pyspark.sql.window import Window

# *** CHANGE ME ***
BOOTSTRAP_SERVERS = "<your_bootstrap_server>:9098"
READ_TOPIC = "topicA"
WRITE_TOPIC = "topicB"


def main():
    spark = SparkSession \
        .builder \
        .appName("03-example-kafka") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    df_sales = read_from_kafka(spark)

    summarize_sales(df_sales)


def read_from_kafka(spark):
    options_read = {
        "kafka.bootstrap.servers":
            BOOTSTRAP_SERVERS,
        "subscribe":
            READ_TOPIC,
        "startingOffsets":
            "earliest",
        "endingOffsets":
            "latest",
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
        .read \
        .format("kafka") \
        .options(**options_read) \
        .load()

    return df_sales


def summarize_sales(df_sales):
    options_write = {
        "kafka.bootstrap.servers":
            BOOTSTRAP_SERVERS,
        "topic":
            WRITE_TOPIC,
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }

    schema = StructType([
        StructField("payment_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", FloatType(), False),
        StructField("payment_date", TimestampType(), False),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("country", StringType(), False),
    ])

    window = Window.partitionBy("country").orderBy("amount")
    window_agg = Window.partitionBy("country")

    df_sales \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .withColumn("row", F.row_number().over(window)) \
        .withColumn("orders", F.count(F.col("amount")).over(window_agg)) \
        .withColumn("sales", F.sum(F.col("amount")).over(window_agg)) \
        .filter(F.col("row") == 1).drop("row") \
        .select("country",
                F.format_number("sales", 2).alias("sales"),
                F.format_number("orders", 0).alias("orders")) \
        .coalesce(1) \
        .orderBy(F.regexp_replace("sales", ",", "").cast("float"), ascending=False) \
        .select(F.to_json(F.struct("*"))).toDF("value") \
        .write \
        .format("kafka") \
        .options(**options_write) \
        .save()


if __name__ == "__main__":
    main()
