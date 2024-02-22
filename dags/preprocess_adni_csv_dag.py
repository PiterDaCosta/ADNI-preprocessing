"""
ADNI metadata CSV preprocess DAG.

This DAG adds some columns to the exams metadata that's downloaded
with the exams files.
The generated CSV file is a prerequisite for the DAG that preprocess
the exam files. But it's also useful when doing
manual examination of the dataset.

The columns that are added to the original file are:
    - Zip File: Indicates on which Zip file is placed the specified exam.
    - Image Path: The relative path of the exam images, this could be a file
        or directory, depending on the image type.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-20
"""
import pandas as pd
from adni_metadata_augmenter_operator import AdniMetadataAugmenterOperator
from airflow.decorators import dag, task
from airflow.models import Variable
from augmented_csv_aggregator_operator import AugmentedCsvAggregatorOperator
from list_zip_files_operator import ListZipFilesOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}


@dag(
    dag_id='preprocess_adni_csv_dag',
    description='DAG that preprocess the csv associated with an ADNI export',
    schedule=None,
    default_args=default_args,
    tags=["ADNI"],
    catchup=False
)
def PreprocessAdniCsv():
    """
    DAG that preprocesses the CSV associated with an ADNI export.

    Tasks:
    1. list_files: Lists the files in the specified directory containing
        ADNI zipped files.
    2. read_csv_task: Reads the ADNI metadata CSV file.
    3. files_processor_task: Processes the ADNI metadata CSV and extracts
        information from zipped files.
    4. processed_csv_aggregator_task: Aggregates the processed CSV files
        into a single CSV file.

    DAG Structure:
    [list_zip_files, read_csv_task] >> files_processor_task
        >> processed_csv_aggregator_task
    """

    @task()
    def read_csv_task():
        """
        Task that reads the ADNI metadata CSV file.

        Returns:
        pandas.DataFrame: DataFrame containing the content
            of the ADNI metadata CSV.
        """
        return pd.read_csv(Variable.get('adni_metadata_csv_path'))

    list_files = ListZipFilesOperator(
        task_id='list_files',
        folder_path=Variable.get('adni_source_zipped_files_directory')
    )

    csv_file = read_csv_task()
    files_processor = AdniMetadataAugmenterOperator.partial(
        task_id='files_processor_task',
        metadata_csv=csv_file,
    ).expand(adni_file_path=list_files.output)

    csv_aggregator = AugmentedCsvAggregatorOperator(
        task_id='processed_csv_aggregator_task',
        original_csv_path=Variable.get('adni_metadata_csv_path'),
        augmented_csv_files=files_processor.output,
        destination_csv_path=Variable.get('adni_augmented_metadata_csv_path'),
    )

    read_csv_task() >> files_processor >> csv_aggregator


dag = PreprocessAdniCsv()
