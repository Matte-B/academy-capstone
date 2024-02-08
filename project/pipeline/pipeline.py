from pathlib import Path

import json
import boto3

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf

from clean_data import clean_frame

packages = ",".join([
            "org.apache.hadoop:hadoop-aws:3.2.0",
            "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
            "net.snowflake:snowflake-jdbc:3.13.3"
            ])

CONFIG = {
        "spark.jars.packages": packages,
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }

S3_PATH = "s3a://dataminded-academy-capstone-resources/raw/open_aq/"
SNOWFLAKE_SCHEMA = "mathias"
SECRET_ARN = "snowflake/capstone/config"

def get_spark_session(name):
    config = SparkConf().setAll(CONFIG.items())
    spark = SparkSession.builder.config(conf=config).appName(name).getOrCreate()
    return spark

def snowflake_config():
    credentials = get_snowflake_credentials()
    return {
        "sfURL": credentials["URL"],
        "sfUser": credentials["USER_NAME"],
        "sfPassword": credentials["PASSWORD"],
        "sfDatabase": credentials["DATABASE"],
        "sfWarehouse": credentials["WAREHOUSE"],
        "sfRole": credentials["ROLE"],
        "sfSchema": SNOWFLAKE_SCHEMA,
    }

def get_snowflake_credentials():
    sms = boto3.client("secretsmanager", region_name="eu-west-1")
    secret = sms.get_secret_value(SecretId=SECRET_ARN)

    return json.loads(secret["SecretString"])

if __name__ == "__main__":
    spark = get_spark_session("ingest")
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.json(str(S3_PATH))

    clean = clean_frame(df)

    clean.write.format("snowflake").options(**snowflake_config()).option(
        "dbtable", "open_aq"
    ).mode("overwrite").save()
