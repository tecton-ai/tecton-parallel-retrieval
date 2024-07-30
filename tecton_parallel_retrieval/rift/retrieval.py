import copy
import pandas
from typing import List
from datetime import datetime, timedelta

from tecton._internals.querytree_api import get_features_from_params
from tecton_core import data_processing_utils

from tecton_core.query.retrieval_params import GetFeaturesForEventsParams
from tecton.framework.data_frame import TectonDataFrame
from tecton.framework.dataset import SavedDataset
from tecton._internals import materialization_api
from tecton_core.id_helper import IdHelper


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
            while True:
                try:
                    remaining_time = deadline - datetime.now() if deadline else None
                    if remaining_time and remaining_time.total_seconds() <= 0:
                        raise TimeoutError("The operation timed out")

                    j.wait_for_completion(timeout=remaining_time)
                    if j._job.state == 'SUCCESS':
                        break
                except TimeoutError:
                    raise  # Re-raise the TimeoutError
                except Exception:
                    continue

    def to_pandas(self):        
        # Calculate how many jobs have completed.
        success_count = sum(j._job.state == 'SUCCESS' for j in self._jobs)
        assert success_count == len(self._jobs), f"Only {success_count} out of {len(self._jobs)} jobs have completed successfully."
        return pandas.concat([j.get_dataset().to_pandas() for j in self._jobs])
    
    def cancel(self):
        for j in self._jobs:
            j.cancel_job()

class MultiDataset:
    def __init__(self, datasets: List[SavedDataset]):
        self._datasets = datasets

    def to_pandas(self) -> pandas.DataFrame:        
        return pandas.concat([ds.to_pandas() for ds in self._datasets])    
    
    def cancel_jobs(self):
        for ds in self._datasets:
            job_id = IdHelper.to_string(ds._proto.saved_dataset.creation_task_id)
            materialization_api.cancel_dataset_job(ds.name, ds._proto.info.workspace, job_id)

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

def retrieve_dataset(workspace, dataset_name):
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
        raise ValueError(f"No datasets found with name starting with '{dataset_name}:'")
    
    return MultiDataset(datasets)


def cancel_dataset_jobs(workspace, dataset_name):
    multi_dataset = retrieve_dataset(workspace, dataset_name)
    multi_dataset.cancel()

