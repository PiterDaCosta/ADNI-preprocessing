"""
Test module for AdniDataSetPreprocessor operator.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-20
"""
import h5py
from utils.testing_utils import dagrun_setup
from adni_data_set_preprocessor import AdniDataSetPreprocessor


def test_operator_in_dag(dag):
    """
    Test if the AdniDataSetPreprocessor operator is present in the given DAG.

    This test creates an instance of the AdniDataSetPreprocessor operator and
    checks if it is added to the specified DAG.
    """
    task1 = AdniDataSetPreprocessor(
        task_id='csv_preprocessing',
        adni_data_set_data={
            'data_set': '',
            'name': '',
            'src_file_path': '',
            'dst_file_path': ''
        },
        dag=dag,
    )
    assert dag.has_task(task1.task_id)


def test_execute(
        dag,
        augmented_metadata_csv_dicom,
        pets_zip_file_path,
        empty_hdf5_file
):
    """
    Test the execution of the AdniDataSetPreprocessor operator.

    This test checks if the resulting HDF5 file has the expected number
    of entries.
    """
    task_id = 'adni_data_set_processor'
    # TODO: Mock dcm2nii conversion to speed up testing.
    AdniDataSetPreprocessor(
        task_id=task_id,
        adni_data_set_data={
            'data_set': augmented_metadata_csv_dicom,
            'name': 'test',
            'src_file_path': pets_zip_file_path,
            'dst_file_path': empty_hdf5_file
        },
        dag=dag
    )
    dagrun_setup(dag, task_id)
    with h5py.File(empty_hdf5_file, 'r') as file:
        assert len(file['X_nii']) == 3
