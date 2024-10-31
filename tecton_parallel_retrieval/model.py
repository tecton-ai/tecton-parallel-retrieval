import pandas

from datetime import datetime, timedelta
from typing import List

from tecton.framework.dataset import SavedDataset
from tecton._internals import materialization_api
from tecton_core.id_helper import IdHelper


class MultiDatasetJob:
    def __init__(self, jobs):
        self._jobs = jobs

    def wait_for_all_jobs(self, timeout: timedelta = None):
        deadline = datetime.now() + timeout if timeout else None

        for j in self._jobs:
            remaining_time = deadline - datetime.now() if deadline else None
            if remaining_time and remaining_time.total_seconds() <= 0:
                raise TimeoutError("The operation timed out")

            j.wait_for_completion(timeout=remaining_time)

    def _ensure_completed(self):
        # Calculate how many jobs have completed.
        success_count = sum(j.state == 'SUCCESS' for j in self._jobs)
        assert success_count == len(
            self._jobs
        ), f"Only {success_count} out of {len(self._jobs)} jobs have completed successfully."

    def to_pandas(self):
        self._ensure_completed()
        return MultiDataset([j.get_dataset() for j in self._jobs]).to_pandas()

    def to_spark(self):
        self._ensure_completed()
        return MultiDataset([j.get_dataset() for j in self._jobs]).to_spark()

    def cancel(self):
        for j in self._jobs:
            j.cancel()


class MultiDataset:
    def __init__(self, datasets: List[SavedDataset]):
        self._datasets = datasets

    def to_pandas(self) -> pandas.DataFrame:
        return pandas.concat([ds.to_dataframe().to_pandas() for ds in self._datasets])

    def to_spark(self) -> "pyspark.sql.DataFrame":
        spark_dfs = [ds.to_dataframe().to_spark() for ds in self._datasets]
        union_df = spark_dfs[0]
        for df in spark_dfs[1:]:
            union_df = union_df.union(df)

        return union_df

    def cancel_jobs(self):
        for ds in self._datasets:
            job_id = IdHelper.to_string(ds._proto.saved_dataset.creation_task_id)
            materialization_api.cancel_dataset_job(
                workspace=ds._proto.info.workspace,
                job_id=job_id,
                dataset=ds.name
            )
