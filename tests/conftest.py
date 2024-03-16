"""
Test Fixtures for Apache Airflow DAG and Operator Unit Tests.

This module contains fixtures and setup code for unit testing Apache Airflow
DAGs and Operators related to the ADNI preprocess project.

Fixtures:
    reset_db: Fixture to reset the Airflow metastore for every test session.
    dag: Fixture providing a sample Apache Airflow DAG for testing.
    pets_metadata_csv_path: Fixture providing the path to the pets metadata
        CSV file.
    pets_metadata_csv: Fixture providing a pandas DataFrame from the pets
        metadata CSV.
    pets_zip_file_path: Fixture providing the path to the pets ZIP file.
    augmented_metadata_csv: Fixture providing an augmented version of the pets
        metadata CSV for testing.
    augmented_metadata_csv_dicom: Fixture providing an augmented version of the
        pets metadata CSV with DICOM file paths for testing.
    empty_hdf5_file: Fixture providing the path to an empty HDF5 file for
        testing.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-20
"""

import os
import shutil
import uuid
import pandas as pd
import pytest
from datetime import datetime
from airflow.models import DAG

# Don't want anything to "magically" work
os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"
# Don't want anything to "magically" work
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
# Set default test settings, skip certain actions, etc.
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
# Hardcode AIRFLOW_HOME to root of this project
os.environ["AIRFLOW_HOME"] = os.path.dirname(os.path.dirname(__file__))

# Settings specific to ADNI preprocess project.
os.environ["AIRFLOW_VAR_ADNI_WORKING_DIRECTORY"] = '/tmp'


@pytest.fixture(autouse=True, scope="session")
def reset_db():
    """Reset the Airflow metastore for every test session."""
    from airflow.utils import db

    db.resetdb()
    yield


@pytest.fixture
def dag():
    """
    Fixture providing a sample Apache Airflow DAG for testing.

    Returns:
        DAG: Apache Airflow DAG instance.
    """
    return DAG(
        str(uuid.uuid4()),
        default_args={
            'owner': 'airflow',
            'start_date': datetime(2022, 1, 1)
        },
        schedule_interval='@daily',
    )


@pytest.fixture
def pets_metadata_csv_path():
    """
    Fixture providing the path to the pets metadata CSV file.

    Returns:
        str: Path to the pets metadata CSV file.
    """
    return os.path.join(os.path.dirname(__file__), 'test_data', 'pet_list.csv')


@pytest.fixture
def pets_metadata_csv(pets_metadata_csv_path):
    """
    Fixture providing a pandas DataFrame from the pets metadata CSV.

    Yields:
        pd.DataFrame: Pandas DataFrame representing the pets metadata.
    """
    yield pd.read_csv(pets_metadata_csv_path)


@pytest.fixture
def pets_zip_file_path():
    """
    Fixture providing the path to the pets ZIP file.

    Returns:
        str: Path to the pets ZIP file.
    """
    return os.path.join(
        os.path.dirname(__file__),
        'test_data',
        'PET_EXAMPLE.zip'
    )


@pytest.fixture
def augmented_metadata_csv(pets_metadata_csv):
    """
    Augmented version of the pets metadata CSV for testing.

    Yields:
        pd.DataFrame: Augmented version of the pets metadata CSV.
    """
    part_count = 3
    third_length = len(pets_metadata_csv) // part_count
    for i in range(part_count):
        start_index = i * third_length
        end_index = (i + 1) * third_length
        pets_metadata_csv.loc[start_index:end_index, 'Zip File'] \
            = "file{}.zip".format(i)
        pets_metadata_csv.loc[start_index:end_index, 'Image Path'] \
            = "tmp/test_{}.nii".format(i)
    return pets_metadata_csv


@pytest.fixture
def augmented_metadata_csv_dicom(pets_metadata_csv, pets_zip_file_path):
    """
    Augmented version of the pets metadata CSV with DICOM file paths.

    Returns:
        pd.DataFrame: Augmented version of the pets metadata CSV with DICOM
            file paths.
    """
    base_path = os.path.join(
        os.path.dirname(__file__),
        'test_data',
        'PET_EXAMPLE',
        'ADNI'
    )
    pets_metadata_csv['Image Path'] = None
    pets_metadata_csv['Zip File'] = None
    values = {
        5: '941_S_1004/ADNI_Brain_PET__Raw/2018-01-30_12_28_14.0/I1000004',
        6: '941_S_1007/ADNI_Brain_PET__Raw/2018-01-30_08_30_44.0/I1000007',
        7: '941_S_1007/ADNI_Brain_PET__Raw/2018-03-30_08_30_44.0/I1000008'
    }
    for index, exam_path in values.items():
        pets_metadata_csv.at[index, 'Image Path'] = os.path.join(
            base_path,
            exam_path
        )
        pets_metadata_csv.at[index, 'Zip File'] = pets_zip_file_path
    return pets_metadata_csv[pets_metadata_csv["Zip File"].notna()]


@pytest.fixture
def empty_hdf5_file(tmp_path):
    """
    Fixture providing the path to an empty HDF5 file for testing.

    Returns:
        str: Path to the empty HDF5 file.
    """
    src = os.path.join(os.path.dirname(__file__), 'test_data', 'empty.hdf5')
    dst = os.path.join(tmp_path, str(uuid.uuid4()) + '.hdf5')
    shutil.copyfile(src, dst)
    return dst


@pytest.fixture
def hrrt_dir_path():
    """
    Fixture providing the path to an example HRRT.

    Returns:
        str: Path to the HRRT directory.
    """
    return os.path.join(
        os.path.dirname(__file__),
        'test_data',
        'HRRT'
    )
