"""
Test module for AugmentedCsvAggregatorOperator operator.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
import os
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from augmented_csv_aggregator_operator import AugmentedCsvAggregatorOperator
from utils.testing_utils import dagrun_setup


def test_operator_in_dag(dag):
    """
    Test if the AugmentedCsvAggregatorOperator is present in the given DAG.

    This test creates an instance of the AdniDataSetPreprocessor and
    checks if it is added to the specified DAG.
    """
    task1 = AugmentedCsvAggregatorOperator(
        task_id='csv_aggregator',
        augmented_csv_files=[],
        destination_csv_path='dst.csv',
        dag=dag,
    )
    assert dag.has_task(task1.task_id)


@pytest.fixture
def example_csvs(augmented_metadata_csv):
    """
    Split the augmented metadata CSV into example parts.

    This function takes the augmented metadata CSV and divides it into multiple
    parts to simulate separate example CSVs. The division is based on the
    specified part count, and each part is a subset of the original CSV.

    Parameters:
        augmented_metadata_csv (pd.DataFrame): Augmented metadata CSV to be
            split into example parts.

    Returns:
        list: A list containing parts of the augmented metadata CSV as separate
            DataFrames.
    """
    part_count = 3
    third_length = len(augmented_metadata_csv) // part_count
    parts = []
    for i in range(part_count):
        start_index = i * third_length
        end_index = (i + 1) * third_length
        parts.append(augmented_metadata_csv.iloc[start_index:end_index].copy())
    return parts


def test_execute(
        dag,
        pets_metadata_csv_path,
        augmented_metadata_csv,
        example_csvs,
        tmp_path
):
    """
    Test the execution of the AugmentedCsvAggregatorOperator operator.

    This tests checks id the new colums are populated correctly.
    """
    task_id = 'augmented_csv_aggregation'
    destination_path = os.path.join(tmp_path, 'dst.csv')
    AugmentedCsvAggregatorOperator(
        task_id=task_id,
        original_csv_path=pets_metadata_csv_path,
        augmented_csv_files=example_csvs,
        destination_csv_path=destination_path,
        dag=dag,
    )
    dagrun_setup(dag, task_id)
    aggregated_csv = pd.read_csv(destination_path)
    assert_frame_equal(
        augmented_metadata_csv,
        aggregated_csv,
        check_dtype=False
    )
