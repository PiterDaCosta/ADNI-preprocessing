"""
Test module for AdniMetadataAugmenterOperator operator.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
import os
import pandas as pd
from utils.testing_utils import dagrun_setup
from adni_metadata_augmenter_operator import AdniMetadataAugmenterOperator


def test_operator_in_dag(dag, pets_zip_file_path):
    """
    Test if the AdniDataSetPreprocessor operator is present in the given DAG.

    This test creates an instance of the AdniMetadataAugmenterOperator and
    checks if it is added to the specified DAG.
    """
    task1 = AdniMetadataAugmenterOperator(
        task_id='adni_metadata_augmenter_processor',
        metadata_csv=pd.DataFrame,
        adni_file_path=pets_zip_file_path,
        dag=dag,
    )
    assert dag.has_task(task1.task_id)


def test_execute(dag, pets_metadata_csv, pets_zip_file_path, tmp_path):
    """
    Test the execution of the AdniMetadataAugmenterOperator Operator.

    This tests checks id the new colums are populated correctly.
    """
    task_id = 'csv_preprocessing'
    AdniMetadataAugmenterOperator(
        task_id=task_id,
        adni_file_path=pets_zip_file_path,
        metadata_csv=pets_metadata_csv,
        destination_path=tmp_path,
        dag=dag,
    )
    result_df = dagrun_setup(dag, task_id)

    ids = ['I168060', 'I42882', 'I47843']
    assert all([
        item in result_df.columns
        for item
        in ['Image Path', 'Zip File']
    ])

    assert all(result_df.loc[result_df['Image Data ID'].isin(ids)]['Zip File']
               == pets_zip_file_path)
    assert all(
        result_df.loc[~result_df['Image Data ID'].isin(ids)]['Zip File']
        .isnull()
    )

    values = {
        5: '941_S_1311/ADNI_Brain_PET__Raw/2007-03-30_12_18_12.0/I47843',
        6: '941_S_1195/ADNI_Brain_PET__Raw/2010-03-02_13_30_46.0/I168060',
        7: '941_S_1195/ADNI_Brain_PET__Raw/2007-03-06_08_33_44.0/I42882'
    }
    for index, exam_path in values.items():
        path = os.path.join(tmp_path, 'ADNI', exam_path)
        assert result_df.iloc[index]['Image Path'] == path

    assert all(
        result_df.loc[~result_df['Image Data ID'].isin(ids)]['Image Path']
        .isnull()
    )
