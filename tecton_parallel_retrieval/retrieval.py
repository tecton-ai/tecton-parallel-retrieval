import argparse

import tecton
from pyspark.sql import functions as F, SparkSession


def retrieval_task():
    parser = argparse.ArgumentParser()

    parser.add_argument('--workspace_name', required=True)
    parser.add_argument('--feature_service_name', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--spine_path', required=True)
    parser.add_argument('--spine_partition_col', required=True)
    parser.add_argument('--spine_partition_names', required=True)
    parser.add_argument('--spine_timestamp_range', required=True)
    parser.add_argument('--timestamp_key', required=True)
    args = parser.parse_args()

    write_output(args.workspace_name, args.feature_service_name, args.output_path, args.spine_path, args.spine_partition_col, args.spine_partition_names, args.spine_timestamp_range, args.timestamp_key)


def write_output(workspace_name, feature_service_name, output_path, spine_path, spine_partition_col, spine_partition_names, spine_timestamp_range, timestamp_key):
    ws = tecton.get_workspace(workspace_name)
    fs = ws.get_feature_service(feature_service_name)
    spark = SparkSession.builder.getOrCreate()
    spine = spark.read.parquet(spine_path)
    output_path = output_path

    if spine_partition_col:
        spine_partition_list = spine_partition_names.split(",")
        spine = spine.filter(F.col(spine_partition_col).isin(spine_partition_list))
    else:
        start_time, end_time = spine_timestamp_range.split(",")
        spine = spine.filter((F.col(timestamp_key) >= start_time) & (F.col(timestamp_key) < end_time))

    df = fs.get_historical_features(
        spine=spine,
        timestamp_key=timestamp_key
    ).to_spark()
    df = df.withColumn("ds", F.to_date(df[timestamp_key]))
    df.write.option("partitionOverwriteMode", "dynamic").partitionBy("ds").parquet(output_path, mode="overwrite")