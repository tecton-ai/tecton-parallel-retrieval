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
    parser.add_argument('--databricks_secret_for_tecton_api_token', required=False, default=None)
    args = parser.parse_args()

    write_output(args.workspace_name, args.feature_service_name, args.output_path, args.spine_path, args.spine_partition_col, args.spine_partition_names, args.spine_timestamp_range, args.timestamp_key, args.databricks_secret_for_tecton_api_token)


def _set_tecton_credentials(databricks_secret_for_tecton_api_token: str):
    print("Fetching credentials from Databricks secret scope")
    api_token = None
    for scope in tecton.conf._get_secret_scopes():
        value = tecton.conf._get_from_db_secrets(databricks_secret_for_tecton_api_token, scope)
        if value is not None:
            api_token = value
            break
    if api_token is None:
        raise Exception(f"Secret {databricks_secret_for_tecton_api_token} not found in any of the secret scopes")
    tecton.set_credentials(tecton_api_key=api_token)
    print("Successfully set Tecton credentials")


def write_output(workspace_name, feature_service_name, output_path, spine_path, spine_partition_col, spine_partition_names, spine_timestamp_range, timestamp_key, databricks_secret_for_tecton_api_token):
    print("Retrieving features from Tecton")
    if databricks_secret_for_tecton_api_token:
        _set_tecton_credentials(databricks_secret_for_tecton_api_token)
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