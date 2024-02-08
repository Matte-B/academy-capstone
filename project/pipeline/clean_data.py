from fetch_data import *
from pyspark.sql import DataFrame, SparkSession

def clean_frame(frame: DataFrame) -> DataFrame:
    pass


if __name__ == "__main__":
    frame = fetch_data()
    frame.show(5)
    frame.printSchema()