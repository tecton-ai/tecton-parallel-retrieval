# Tecton Parallel Retrieval (Experimental)

Tecton Parallel Retrieval is an experimental feature that allows you to
retrieve feature values in parallel, using multiple Databricks Spark
clusters. This feature is currently in alpha and is subject to change.

This can be found on pypi https://pypi.org/project/tecton-parallel-retrieval/

## How to Run

```python

import tecton_parallel_retrieval
tecton_parallel_retrieval.run_parallel_query(
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

## How to Install and Upload

To generate a whl file run the following in your terminal...

```bash

pip install --upgrade build
build

```

to upload it to pypi...

```bash

pip install --upgrade twine
twine upload --repository pypi dist/*

```

make sure you update the version in setup.py as well
