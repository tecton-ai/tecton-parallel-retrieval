# Tecton Parallel Retrieval (Experimental)

Tecton Parallel Retrieval is an experimental feature that allows you to
retrieve feature values in parallel, using multiple compute clusters (works for Spark and Rift). This feature is currently in alpha and is subject to change.

This can be found on pypi https://pypi.org/project/tecton-parallel-retrieval/

## How to Run (Spark)

```python
import tecton
import tecton_parallel_retrieval as retrieval

ws = tecton.Workspace('prod')
feature_service = ws.get_feature_service('my_feature_service')

spine = spark.read.parquet('s3://...')

df = feature_service.get_features_for_events(spine)

multi_job = retrieval.start_dataset_jobs_in_parallel(
    df,
    dataset_name="tecton-parallel-retrieval-test",
    num_splits=5,
    compute_mode='spark',
    staging_path='s3://bucket/staging',  # Materialization job role should have read access
    tecton_materialization_runtime="1.0.10",
    cluster_config=tecton.EMRClusterConfig(instance_type='m5.8xlarge', spark_config={'spark.sql.shuffle.partitions': '5000'})
)

multi_job.wait_for_all_jobs()

multi_job.to_spark()
```

## How to Run (Rift)

```python
import tecton
import tecton_parallel_retrieval as retrieval

ws = tecton.Workspace('prod')
feature_service = ws.get_feature_service('my_feature_service')

spine = pandas.read_parquet('...')

df = feature_service.get_features_for_events(spine)

multi_job = retrieval.start_dataset_jobs_in_parallel(
    df,
    dataset_name="tecton-parallel-retrieval-test",
    num_splits=5,
    compute_mode='rift',
    environment='rift-core-1.0',
    cluster_config=tecton.RiftBatchConfig(instance_type='m5.8xlarge')
)

multi_job.wait_for_all_jobs()

multi_job.to_pandas()

```

## How to retrieve existing parallel dataset

```python
import tecton
import tecton_parallel_retrieval as retrieval

ws = tecton.Workspace('prod')

multi_ds = retrieval.retrieve_dataset(ws, "tecton-parallel-retrieval-test")
multi_ds.to_pandas()
```

## How to Install and Upload

To generate a whl file run the following in your terminal...

```bash

pip install --upgrade build
python -m build

```

The following command uploads it to pypi.
Make sure you update the version in setup.py, as well as the dependency versions in setup.py and the hardcoded job dependencies in main.py

```bash

pip install --upgrade twine
twine upload --repository pypi dist/*

```
