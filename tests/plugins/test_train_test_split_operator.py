"""
Test module for TrainTestSplitOperator operator.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
from plugins.train_test_split_operator import TrainTestSplitOperator
from utils.testing_utils import dagrun_setup


def test_execute(dag, augmented_metadata_csv):
    """
    Test the execution of the TrainTestSplitOperator operator.

    This tests checks that only specified files are taken into consideration
    and that their size are correct.
    """
    task_id = 'train_test_split_task'
    TrainTestSplitOperator(
        task_id=task_id,
        metadata_csv=augmented_metadata_csv,
        adni_files_list=['file1.zip', 'file2.zip'],
        dag=dag,
    )

    resulting_csvs = dagrun_setup(dag, task_id)

    assert len(resulting_csvs["train_csv"]) == 4
    assert len(resulting_csvs["test_csv"]) == 2

    assert not any(resulting_csvs['train_csv']['Zip File'] == 'file0.zip')
    assert not any(resulting_csvs['test_csv']['Zip File'] == 'file0.zip')
