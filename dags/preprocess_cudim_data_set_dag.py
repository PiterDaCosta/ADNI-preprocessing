"""
CUDIM preprocess exams files.

This DAG preprocess the provided CUDIM PET exams stored in a zip file.
The result of this preprocessing is stored on
hdf5 files.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-03-16
"""
import h5py
import os
import pandas as pd
import random
from adni_data_set_preprocessor import AdniDataSetPreprocessor
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}


@dag(
    dag_id='preprocess_cudim_data_set_dag',
    description=(
            'DAG that preprocess CUDIM PET files and creates h5df files '
            'ready to be used on ML projects.'
    ),
    schedule=None,
    default_args=default_args,
    tags=["ADNI"],
    catchup=False
)
def PreprocessCudimDataSet():
    """
    CUDIM PET files preprocessor.

    DAG that preprocesses CUDIM PET files and creates hdf5 files ready to be
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
    random.seed(103)

    @task()
    def read_csv_task():
        return pd.read_csv('shared/src-data/CUDIM/cudim-metadata.csv')

    @task()
    def prepare_csv_rows(metadata_csv):
        metadata_csv = metadata_csv[metadata_csv['AccessionNumber'].notna()]
        metadata_csv['AccessionNumber'] = metadata_csv['AccessionNumber'].apply(lambda a: str(int(a)))
        metadata_csv = metadata_csv[~metadata_csv['AccessionNumber'].isin([
            # These folders are empty on the csv.
            '1702505489', '1703017605', '1703105436', '1703105511', '1703105571'
        ])]
        metadata_csv['Image Path'] = metadata_csv['AccessionNumber'].apply(lambda a: "shared/temporary/extracted/CUDIM-data/studies/" + a)
        metadata_csv['Format'] = 'DCM'
        metadata_csv = metadata_csv.rename(columns={'AccessionNumber': 'Image Data ID', 'edad': 'Age', 'sexo': 'Sex'})
        # return metadata_csv[:7]
        return metadata_csv


    @task()
    def initialize_hdf5_files(labeling_criteria):
        dst_path = Variable.get('adni_output_hdf5_files_directory')
        for criteria in labeling_criteria:
            file_name = criteria + ".hdf5"
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

    @task()
    def create_data_sets(labeling_criteria, metadata_csv):
        list_of_files = []
        dst_path = Variable.get('adni_output_hdf5_files_directory')
        for criteria in labeling_criteria:
            if criteria == 'pre_pet_diag':
                label_column = 'Diagnóstico previo al PET'
            else:
                label_column = 'Diagnóstico posterior al PET'
            filtered_data_set = metadata_csv[metadata_csv[label_column].isin(['N', 'MCI', 'AD'])]
            filtered_data_set = filtered_data_set.rename(columns={label_column: 'Group'})
            filtered_data_set.to_csv('shared/temp.csv')
            # filtered_data_set = filtered_data_set.iloc[19:, :]
            # filtered_data_set = filtered_data_set.iloc[23:, :]
            # filtered_data_set = filtered_data_set.iloc[35:, :]
            # filtered_data_set.to_csv('shared/temp2.csv')
            file_name = criteria + ".hdf5"
            file_path = os.path.join(dst_path, file_name)
            list_of_files.append(
                {
                    'data_set': filtered_data_set,
                    'name': criteria,
                    'src_file_path': 'shared/src-data/CUDIM/CUDIM-data.zip',
                    'dst_file_path': file_path
                }
            )
        return list_of_files

    labeling_criteria = ['pre_pet_diag', 'post_pet_diag']

    initialize_hdf5_files(labeling_criteria)
    csv_file = read_csv_task()
    filtered_csv = prepare_csv_rows(csv_file)
    data_sets = create_data_sets(labeling_criteria, filtered_csv)

    AdniDataSetPreprocessor.partial(
        task_id='cudim_data_set_preprocessor',
    ).expand(adni_data_set_data=data_sets)


dag = PreprocessCudimDataSet()
