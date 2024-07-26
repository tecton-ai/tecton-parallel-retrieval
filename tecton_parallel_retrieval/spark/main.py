import datetime
import sys
from typing import Optional

from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F, SparkSession
from databricks.sdk.service.jobs import SubmitTask, PythonWheelTask
from databricks.sdk.service.compute import ClusterSpec
from databricks.sdk.service.compute import Library
from databricks.sdk.service.compute import PythonPyPiLibrary
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import AwsAttributes
import tecton
import time

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s- %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABRICKS_TERMINAL_STATE_NAMES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]

def get_partition_names(spark, path, partition_col):
    try:
        parts = spark.read.parquet(path).select(partition_col).distinct().collect()
        parts = {str(r[partition_col]) for r in parts}
        return parts
    except AnalysisException:
        return set()

def generate_splits(spark, spine_path, spine_partition_col, num_splits, output_path, timestamp_key):
    spine = spark.read.parquet(spine_path)
    if spine_partition_col is None:
        tecton_ds_col = "__tecton_util_ds"
        spine = spine.withColumn(tecton_ds_col, F.to_date(spine[timestamp_key]))
    else:
        tecton_ds_col = spine_partition_col
    logger.info("Generating splits via computing data counts")
    spine_with_date_counts = spine.groupBy(tecton_ds_col).count().collect()
    logger.info("Looking for existing partitions in output_path")
    existing_partitions = get_partition_names(spark, output_path, "ds")
    logger.info("Found existing partitions: " + str(sorted(list(existing_partitions))))
    # Calculate remaining spine partitions to compute ghf for
    spine_with_date_counts = [
        r for r in spine_with_date_counts
        if str(r[tecton_ds_col]) not in existing_partitions
    ]
    if len(spine_with_date_counts) == 0:
        # This time range has already been computed and written to the output path
        return []
    sorted_data_counts = sorted(spine_with_date_counts, key=lambda r: r[tecton_ds_col])
    total_sum = sum(r["count"] for r in sorted_data_counts)
    count_per_split = total_sum / num_splits

    if spine_partition_col is not None:
        splits = [[]]
        split_count = 0

        for row in sorted_data_counts:
            if split_count >= count_per_split:
                split_count = 0
                splits.append([])
            splits[-1].append(str(row[tecton_ds_col]))
            split_count += row["count"]

        splits = [
            {
                "spine_partition_col": spine_partition_col,
                "spine_partition_names": ",".join(s),
                "spine_timestamp_range": "",
                "name": f"{idx + 1}/{len(splits)}"
            } for idx, s in enumerate(splits)
        ]
    else:
        splits = []
        # Calculate time range splits for spine. Start time is inclusive, end time is exclusive.
        split_start = sorted_data_counts[0][tecton_ds_col]
        split_count = sorted_data_counts[0]["count"]
        for row in sorted_data_counts[1:]:
            if split_count > count_per_split:
                splits.append((split_start, row[tecton_ds_col]))
                split_count = 0
                split_start = row[tecton_ds_col]
            split_count += row["count"]
        split_end_final = sorted_data_counts[-1][tecton_ds_col] + datetime.timedelta(days=1)
        splits.append((split_start, split_end_final))

        splits = [
            {
                "spine_partition_col": "",
                "spine_partition_names": "",
                "spine_timestamp_range": str(s[0]) + "," + str(s[1]),
                "name": f"{idx + 1}/{len(splits)}"
            } for idx, s in enumerate(splits)
        ]
    logger.info(f"Generated {len(splits)} splits")
    for split in splits:
        d = {}
        if split["spine_partition_names"]:
            d["spine_partition_names"] = split["spine_partition_names"]
        if split["spine_timestamp_range"]:
            d["spine_timestamp_range"] = split["spine_timestamp_range"]
        logger.info(f"Split {split['name']}: {d}")
    return splits


def get_cluster_spec(
        databricks_instance_profile_arn: str,
        databricks_policy_id: Optional[str],
        databricks_runtime_version: str,
        databricks_driver_node_type: str,
        databricks_worker_node_type: str,
        databricks_worker_node_count: int
):
    aws_attr = AwsAttributes(
        instance_profile_arn=databricks_instance_profile_arn
    )
    spark_env_vars = {}
    if tecton.conf.get_or_none("TECTON_CLUSTER_NAME"):
        spark_env_vars["TECTON_CLUSTER_NAME"] = tecton.conf.get_or_raise("TECTON_CLUSTER_NAME")
    cluster_spec = ClusterSpec(
        apply_policy_default_values=True,
        driver_node_type_id=databricks_driver_node_type,
        node_type_id=databricks_worker_node_type,
        num_workers=databricks_worker_node_count,
        spark_version=databricks_runtime_version,
        spark_env_vars=spark_env_vars,
        enable_elastic_disk=True,
        aws_attributes=aws_attr,
        policy_id=databricks_policy_id
    )
    return cluster_spec


def launch_job(databricks_client, split, cluster_spec, workspace_name, feature_service_name, timestamp_key, spine_path, output_path, databricks_secret_for_tecton_api_token):
    job_params = {
        "feature_service_name": feature_service_name,
        "workspace_name": workspace_name,
        "spine_path": spine_path,
        "spine_partition_col": split["spine_partition_col"],
        "spine_partition_names": split["spine_partition_names"],
        "spine_timestamp_range": split["spine_timestamp_range"],
        "output_path": output_path,
        "timestamp_key": timestamp_key,
    }
    if databricks_secret_for_tecton_api_token:
        job_params["databricks_secret_for_tecton_api_token"] = databricks_secret_for_tecton_api_token
    wheel_task = PythonWheelTask(
        entry_point="retrieval_task",
        package_name="tecton_parallel_retrieval",
        named_parameters=job_params
    )
    task = SubmitTask(
        task_key="Task1",
        new_cluster=cluster_spec,
        python_wheel_task=wheel_task,
        libraries=[
            Library(pypi=PythonPyPiLibrary(package="tecton==0.9.*")),
            Library(pypi=PythonPyPiLibrary(package="tecton-parallel-retrieval==0.1.0b1"))
        ]
    )
    split_name = split["name"]
    job = databricks_client.jobs.submit(
        run_name=f"Tecton Parallel Run {split_name} for {workspace_name}:{feature_service_name}",
        tasks=[
            task
        ]
    )
    return job

def launch_jobs_and_wait(databricks_client, splits, parallel_job_count, cluster_spec, workspace_name, feature_service_name, timestamp_key, spine_path, output_path, databricks_secret_for_tecton_api_token):
    active_jobs = {}
    remaining_splits = splits[::-1]
    while remaining_splits or active_jobs:
        runs_updated = False
        # poll jobs
        for run_id, split in list(active_jobs.items()):
            run_info = databricks_client.jobs.get_run(run_id)
            if run_info.state.life_cycle_state.name in DATABRICKS_TERMINAL_STATE_NAMES:
                success = run_info.state.result_state.name == "SUCCESS"
                logger.info(f"Run {run_id} finished with success={success} for split {split['name']}")
                if not success:
                    run_output = databricks_client.jobs.get_run_output(run_info.tasks[0].run_id)
                    logger.info(f"Error: {run_output.error} {run_output.error_trace}")
                    remaining_splits.append(split)
                    logger.info(f"Retrying split {split['name']}")
                del active_jobs[run_id]
                runs_updated = True

        # launch jobs
        while remaining_splits and len(active_jobs) < parallel_job_count:
            split = remaining_splits.pop()
            job = launch_job(databricks_client, split, cluster_spec, workspace_name, feature_service_name, timestamp_key, spine_path, output_path, databricks_secret_for_tecton_api_token)
            run_id = job.response.run_id
            active_jobs[run_id] = split
            logger.info(f"Launched run {run_id} for split {split['name']}")
            runs_updated = True

        if runs_updated:
            active_job_string = {
                split["name"]: run_id for run_id, split in active_jobs.items()
            }
            logger.info(f"Active jobs: {active_job_string}")
            split_names = sorted([split["name"] for split in remaining_splits])
            logger.info(f"Queued_splits: {split_names}")

        time.sleep(60)


def run_parallel_query(
        workspace_name: str,
        feature_service_name: str,
        timestamp_key: str,
        spine_path: str,
        spine_partition_col: Optional[str],
        output_path: str,

        max_splits: int,
        max_parallel_jobs: int,

        databricks_instance_profile_arn: str,
        databricks_policy_id: Optional[str],
        databricks_runtime_version: str,
        databricks_driver_node_type: str,
        databricks_worker_node_type: str,
        databricks_worker_node_count: int,
        databricks_secret_for_tecton_api_token: Optional[str] = None
):
    spark = SparkSession.builder.getOrCreate()
    databricks_client = WorkspaceClient()
    cluster_spec = get_cluster_spec(
        databricks_instance_profile_arn=databricks_instance_profile_arn,
        databricks_policy_id=databricks_policy_id,
        databricks_runtime_version=databricks_runtime_version,
        databricks_driver_node_type=databricks_driver_node_type,
        databricks_worker_node_type=databricks_worker_node_type,
        databricks_worker_node_count=databricks_worker_node_count)
    splits = generate_splits(spark, spine_path, spine_partition_col, max_splits, output_path, timestamp_key)
    if len(splits) == 0:
        logger.info("Data already exists in the output_path for the time range provided in the spine. No jobs will be launched.")
        return
    launch_jobs_and_wait(databricks_client, splits, max_parallel_jobs, cluster_spec, workspace_name, feature_service_name, timestamp_key, spine_path, output_path, databricks_secret_for_tecton_api_token)
    logger.info("Completed successfully")
