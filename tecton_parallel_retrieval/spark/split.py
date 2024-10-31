import logging
import uuid

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tecton.tecton_context import TectonContext


logger = logging.getLogger(__name__)


def split_spine(spine: DataFrame, join_keys: List[str], num_splits: int, staging_path: str) -> List[str]:
    assert isinstance(spine, DataFrame), "spine must be a pyspark.sql.DataFrame when compute is Spark"

    destination = f"{staging_path.rstrip('/')}/{uuid.uuid4().hex}"
    logger.info(f"Splitting spine into {num_splits} parts and staging to {destination}")
    spine.repartitionByRange(num_splits, *join_keys).write.parquet(destination)

    logger.info("Retrieving list of spine files")
    spark_session = TectonContext.get_instance()._get_spark()
    created_files = spark_session.read.parquet(destination)\
        .select(F.input_file_name().alias('file_name'))\
        .distinct()\
        .collect()
    created_files = [r['file_name'] for r in created_files]
    return created_files
