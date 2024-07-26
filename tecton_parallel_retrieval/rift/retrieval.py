import copy
import pandas
from typing import List
from datetime import datetime, timedelta

from tecton._internals.querytree_api import get_features_from_params
from tecton_core import data_processing_utils

from tecton_core.query.retrieval_params import GetFeaturesForEventsParams, GetFeaturesInRangeParams
from tecton.framework.data_frame import TectonDataFrame
from tecton.framework import jobs


def split_spine(spine: pandas.DataFrame, join_keys: List[str], split_count: int, strategy: str) -> List[pandas.DataFrame]:
    """
    This is an experimental feature that splits the spine DataFrame into multiple DataFrames based on the spine split
    strategy specified in the configuration. The split strategy can be one of the following:
    - even: Splits the spine into equal-sized chunks
    - minimize_distance: Splits the spine at points where the distance between consecutive keys is maximized
    - agglomerative_clustering: Splits the spine using Agglomerative Clustering based on the bitwise distance between keys
    """

    if strategy == "even":
        return data_processing_utils._even_split(spine, join_keys, split_count)
    elif strategy == "minimize_distance":
        return data_processing_utils._minimize_distance_split(spine, join_keys, split_count)
    elif strategy == "agglomerative_clustering":
        return data_processing_utils._agglomerative_clustering_split(spine, join_keys, split_count)
    else:
        error = f"Unknown spine split strategy: {strategy}"
        raise ValueError(error)

class MultiDatasetJob:
    def __init__(self, jobs):
        self._jobs = jobs

    def wait_for_all_jobs(self, timeout: timedelta = None):
        deadline = datetime.now() + timeout if timeout else None

        for j in self._jobs:
            j.wait_for_completion(timeout=deadline - datetime.now())

    def to_pandas(self):        
        # Calculate how many jobs have completed.
        success_count = sum(j._job.state == 'SUCCESS' for j in self._jobs)
        assert success_count == self._jobs.count, f"Only {success_count} out of {len(self._jobs)} jobs have completed successfully."
        return pandas.concat([j.get_dataset().to_pandas() for j in self._jobs])


def start_dataset_jobs_in_parallel(df: TectonDataFrame, dataset_name, num_splits, split_strategy='even', **kwargs):
    params = df._request_params
    jobs = []

    if isinstance(params, GetFeaturesForEventsParams):
        chunks = split_spine(params.events, params.join_keys, num_splits, strategy=split_strategy)
        for idx, spine_chunk in enumerate(chunks):
            subtask_params = copy.copy(params)
            subtask_params.events = spine_chunk
            subtask_df = get_features_from_params(subtask_params)
            
            job = subtask_df.start_dataset_job(
                dataset_name=f"{dataset_name}:{idx}", **kwargs
            )
            jobs.append(job)
    else:
        raise RuntimeError("Only get_features_for_events is currently supported")

    return MultiDatasetJob(jobs)