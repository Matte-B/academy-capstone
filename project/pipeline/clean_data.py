from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as sf
from pyspark.sql.types import (TimestampType)

def extract_struct_field(frame, column, fields):
    for field in fields:
        frame = frame.withColumn(field, sf.col(column).getField(field))
    
    dropped_struct_frame = frame.drop(column)
    return dropped_struct_frame

def flatten_frame(frame):
    structs = {
        "coordinates" : ["latitude", "longitude"],
        "date" : ["local", "utc"]
    }

    for struct_column, fields in structs.items():
        frame = extract_struct_field(frame, struct_column, fields)
    
    return frame

def drop_columns(frame):
    drops = ["local", "locationId"]
    return frame.drop(*drops)

def rename_columns(frame):
    renames = {
        "isAnalysis" : "is_analysis",
        "isMobile" : "is_mobile",
        "utc" : "timestamp_utc"
    }

    for old_name, new_name in renames.items():
        frame = frame.withColumnRenamed(old_name, new_name)

    return frame

def cast_columns(frame):
    mapping ={
        TimestampType: ["timestamp_utc"]
    }

    for datatype, colnames in mapping.items():
        for colname in colnames:
            frame = frame.withColumn(
                colname, sf.col(colname).cast(datatype())
            )

    return frame

def clean_frame(frame):
    for transformation in (
        flatten_frame,
        drop_columns,
        rename_columns,
        cast_columns
    ):
        frame = transformation(frame)
    
    return frame