# Purpose: Amazon EMR Serverless and Amazon MSK Serverless Demo
#          Write messages from a CSV file to Kafka topicC
#          to simulate real-time streaming sales data
# Author:  Gary A. Stafford
# Date: 2022-07-24

import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType

# *** CHANGE ME ***
BOOTSTRAP_SERVERS = "<your_bootstrap_server>:9098"
S3_BUCKET = "<your_s3_bucket>"
WRITE_TOPIC = "topicC"
MESSAGE_DELAY = 0.5


def main():
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

    df_sales = read_from_csv(spark, schema)
    df_sales.cache()

    write_to_kafka(spark, df_sales)


def read_from_csv(spark, schema):
    df_sales = spark.read \
        .csv(path=f"s3a://{S3_BUCKET}/sample_data/sales_incremental_large.csv",
             schema=schema, header=True, sep="|")

    return df_sales


def write_to_kafka(spark, df_sales):
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

        time.sleep(MESSAGE_DELAY)  # 2000 messages * 0.5 seconds  / 60 = ~17 minute runtime


if __name__ == "__main__":
    main()
