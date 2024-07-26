# Tecton Parallel Retrieval (Experimental)

Tecton Parallel Retrieval is an experimental feature that allows you to
retrieve feature values in parallel, using multiple Databricks Spark
clusters. This feature is currently in alpha and is subject to change.

This can be found on pypi https://pypi.org/project/tecton-parallel-retrieval/

## How to Run(Spark)

```python

from tecton_parallel_retrieval.spark import run_parallel_query

run_parallel_query(
        spine_path = "s3://path/to/your/spine", # Must be Parquet format
        spine_partition_col="ds", # None if un-partitioned
        output_path = "s3://path/to/output",
        timestamp_key = "timestamp", # timestamp key of spine
        feature_service_name= "your_feature_service",
        workspace_name= "your_workspace",

        max_splits=10, # Number of chunks to break the job up into
        max_parallel_jobs=3, # Number of parallel Databricks clusters that run retrieval

				# Databricks configs
        databricks_instance_profile_arn = "arn:aws:iam::111122223333:instance-profile/your-databricks-instance-profile",
        databricks_driver_node_type = "m5.xlarge",
        databricks_worker_node_type = "m5.8xlarge",
        databricks_policy_id = None,
        databricks_runtime_version = "9.1.x-scala2.12",
        databricks_worker_node_count = 1,
    )

```

## How to Run(Rift)

```python

from tecton_parallel_retrieval.rift import start_dataset_jobs_in_parallel, retrieve_dataset

workspace = tecton.get_workspace('your_workspace')
fv = workspace.get_feature_view('your_feature_view')
df = fv.get_features_for_events(events=spine, timestamp_key='your_timestamp_key')
jobs = retrieval.start_dataset_jobs_in_parallel(df, "your_dataset_name", 3)
jobs.wait_for_all_jobs()
ds = retrieve_dataset(workspace, "your_dataset_name")


```

## How to Install and Upload

To generate a whl file run the following in your terminal...

```bash

pip install --upgrade build
python -m build

```

The following command uploads it to pypi.
Make sure you update the version in setup.py, as well as the dependency versions in setup.py and the hardcoded databricks job dependencies in main.py

```bash

pip install --upgrade twine
twine upload --repository pypi dist/*

```
