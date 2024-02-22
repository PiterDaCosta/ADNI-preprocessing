"""
ADNI preprocess exams files.

This DAG preprocess the ADNI PET exams stored in the zip files downloaded from
https://adni.loni.usc.edu. The result of this preprocessing is stored on
hdf5 files.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
import h5py
import os
import pandas as pd
from adni_data_set_preprocessor import AdniDataSetPreprocessor
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from list_zip_files_operator import ListZipFilesOperator
from train_test_split_operator import TrainTestSplitOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}


@dag(
    dag_id='preprocess_adni_data_set_dag',
    description=(
            'DAG that preprocess ADNI PET files and creates h5df files '
            'ready to be used on ML projects.'
    ),
    schedule=None,
    default_args=default_args,
    tags=["ADNI"],
    catchup=False
)
def PreprocessAdniDataSet():
    """
    ADNI PET files preprocessor.

    DAG that preprocesses ADNI PET files and creates hdf5 files ready to be
        used on ML projects.

    Tasks:
    1. list_files: Lists the files in the specified directory containing
        ADNI zipped files.
    2. read_csv_task: Reads the augmented metadata CSV file
        (see preprocess_adni_csv_dag).
    3. split_train_test: Splits the data into training and testing sets.
    4. create_hdf5_files: Creates HDF5 files for each dataset.
    5. adni_data_set_preprocessor: Processes ADNI datasets, standardizing and
        applying transformations over the exams files.

    DAG Structure:
    [list_files, read_csv_task] >> split_train_test >> create_hdf5_files
        >> adni_data_set_preprocessor
    """
    list_files = ListZipFilesOperator(
        task_id='list_files',
        folder_path=Variable.get('adni_source_zipped_files_directory'),
    )

    @task()
    def read_csv_task():
        return pd.read_csv(Variable.get('adni_augmented_metadata_csv_path'))

    csv_file = read_csv_task()

    split_train_test = TrainTestSplitOperator(
        task_id='split_train_test',
        metadata_csv=csv_file,
        adni_files_list=list_files.output,
    )

    @task()
    def create_hdf5_files(data_sets):
        list_of_files = []
        dst_path = Variable.get('adni_output_hdf5_files_directory')
        for data_set_name in data_sets.keys():
            file_name = data_set_name + ".hdf5"
            file_path = os.path.join(dst_path, file_name)
            if not os.path.exists(dst_path):
                os.makedirs(dst_path)
            h5 = h5py.File(file_path, 'w')
            # Create a variable-length string dtype
            string_dt = h5py.special_dtype(vlen=str)
            h5.create_dataset(
                'ID',
                shape=(0,),
                maxshape=(None,),
                dtype=string_dt
            )
            # TODO: Make the image size parametric.
            h5.create_dataset(
                'X_nii',
                shape=(0, 100, 100, 120),
                maxshape=(None, 100, 100, 120),
                chunks=None
            )

            h5.create_dataset('X_Age', shape=(0,), maxshape=(None,))
            h5.create_dataset('X_Sex', shape=(0,), maxshape=(None,))
            h5.create_dataset('y', shape=(0,), maxshape=(None,))
            h5.close()

            # TODO: Split this into two jobs.
            grouped_by_file = dict(tuple(
                data_sets[data_set_name].groupby("Zip File")
            ))
            for zip_file_path in grouped_by_file.keys():
                list_of_files.append(
                    {
                        'data_set': grouped_by_file.get(zip_file_path),
                        'name': data_set_name,
                        'src_file_path': zip_file_path,
                        'dst_file_path': file_path
                    }
                )
        return list_of_files

    data_sets_with_files = create_hdf5_files(split_train_test.output)

    AdniDataSetPreprocessor.partial(
        task_id='adni_data_set_preprocessor',
    ).expand(adni_data_set_data=data_sets_with_files)


dag = PreprocessAdniDataSet()
