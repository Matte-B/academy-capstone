from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf

CONFIG = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.0",
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }

S3_PATH = "s3a://dataminded-academy-capstone-resources/raw/open_aq/"
SOURCE = "source_data"

def read_data(path: Path):
    config = SparkConf().setAll(CONFIG.items())
    spark = SparkSession.builder.config(conf=config).getOrCreate()

    return spark.read.json(str(path))

def fetch_data():
    # use relative paths, so that the location of this project on your system
    # won't mean editing paths
    project_path = Path(__file__).parents[1]
    resources_dir = project_path / "resources"
    source_data = resources_dir / SOURCE

    if source_data.exists():
        frame = read_data(source_data)
    else:
        frame = read_data(S3_PATH)
        frame.write.json(
            path=str(resources_dir / SOURCE),
            mode="overwrite"
        )

    return frame

