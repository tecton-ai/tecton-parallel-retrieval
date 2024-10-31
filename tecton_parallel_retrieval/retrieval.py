import copy
import pandas

from typing import Union, List, Optional

from tecton_core.query.retrieval_params import GetFeaturesForEventsParams
from tecton_core.compute_mode import ComputeMode, offline_retrieval_compute_mode

from tecton.framework.data_frame import TectonDataFrame
from tecton._internals.querytree_api import get_features_from_params
from tecton._internals.utils import infer_timestamp
from tecton_parallel_retrieval.model import MultiDataset, MultiDatasetJob


def split_spine(
    events: pandas.DataFrame,
    join_keys: List[str],
    num_splits: int,
    compute_mode: ComputeMode,
    staging_path: Optional[str] = None,
) -> List[Union[pandas.DataFrame, str]]:
    if compute_mode == ComputeMode.RIFT:
        from tecton_parallel_retrieval.rift import split as rift_split

        return rift_split.split_spine(events, join_keys, num_splits, strategy='event')

    if compute_mode == ComputeMode.SPARK:
        from tecton_parallel_retrieval.spark import split as spark_split

        if not staging_path:
            raise ValueError("`staging_path` must be provided when compute is Spark")

        return spark_split.split_spine(events, join_keys, num_splits, staging_path)

    raise RuntimeError(f"Unsupported compute mode {compute_mode}")


def start_dataset_jobs_in_parallel(
    df: TectonDataFrame,
    dataset_name: str,
    num_splits,
    compute_mode: Union[ComputeMode, str] = ComputeMode.RIFT,
    staging_path: Optional[str] = None,
    **kwargs
) -> MultiDatasetJob:
    params = df._request_params
    jobs = []
    compute_mode = offline_retrieval_compute_mode(compute_mode)

    if isinstance(params, GetFeaturesForEventsParams):
        timestamp_key = params.timestamp_key or infer_timestamp(params.events)

        chunks = split_spine(params.events,
                             params.join_keys,
                             num_splits,
                             compute_mode,
                             staging_path)
        for idx, spine_chunk in enumerate(chunks):
            subtask_params = copy.copy(params)
            subtask_params.events = spine_chunk
            subtask_params.timestamp_key = timestamp_key
            subtask_df = get_features_from_params(subtask_params)

            job = subtask_df.start_dataset_job(
                dataset_name=f"{dataset_name}:{idx}", **kwargs)
            jobs.append(job)
    else:
        raise RuntimeError(
            "Only get_features_for_events is currently supported")

    return MultiDatasetJob(jobs)


def retrieve_dataset(workspace, dataset_name) -> MultiDataset:
    datasets = []
    for remote_dataset_name in workspace.list_datasets():
        if remote_dataset_name.startswith(dataset_name + ":"):
            datasets.append(workspace.get_dataset(remote_dataset_name))

    # Sort the datasets by the numeric index in the dataset name
    def get_index(ds):
        try:
            return int(ds.name.split(':')[-1])
        except ValueError:
            return float('inf')

    datasets = sorted(datasets, key=get_index)

    if not datasets:
        raise ValueError(
            f"No datasets found with name starting with '{dataset_name}:'")

    return MultiDataset(datasets)


def cancel_dataset_jobs(workspace, dataset_name):
    multi_dataset = retrieve_dataset(workspace, dataset_name)
    multi_dataset.cancel_jobs()
