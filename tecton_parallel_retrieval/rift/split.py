import pandas
from typing import List

from tecton_core import data_processing_utils


def split_spine(spine: pandas.DataFrame, join_keys: List[str], split_count: int, strategy: str) -> List[pandas.DataFrame]:
    """
    This is an experimental feature that splits the spine DataFrame into multiple DataFrames based on the spine split
    strategy specified in the configuration. The split strategy can be one of the following:
    - even: Splits the spine into equal-sized chunks
    - minimize_distance: Splits the spine at points where the distance between consecutive keys is maximized
    - agglomerative_clustering: Splits the spine using Agglomerative Clustering based on the bitwise distance between keys
    """
    assert isinstance(spine, pandas.DataFrame), "spine must be a pandas.DataFrame when compute is Rift"

    if strategy == "even":
        return data_processing_utils._even_split(spine, join_keys, split_count)
    elif strategy == "minimize_distance":
        return data_processing_utils._minimize_distance_split(spine, join_keys, split_count)
    elif strategy == "agglomerative_clustering":
        return data_processing_utils._agglomerative_clustering_split(spine, join_keys, split_count)
    else:
        error = f"Unknown spine split strategy: {strategy}"
        raise ValueError(error)

