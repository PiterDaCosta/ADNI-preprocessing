"""
AdniMetadataAugmenterOperator Class for ADNI exams metadata csv preprocessing.

This class is used to add some extra columns to the ADNI exams metadata csv
file.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
import os
from functools import partial
from utils.adni_csv_helpers import path_from_csv_row
from zip_file_processor_operator import ZipFileProcessorOperator


class AdniMetadataAugmenterOperator(ZipFileProcessorOperator):
    """
    Apache Airflow Operator for Augmenting ADNI Metadata.

    This operator extends the ZipFileProcessorOperator to augment the metadata
    associated with ADNI (Alzheimer's Disease Neuroimaging Initiative)
    exam files stored on 'metadata_csv' parameter. It adds an 'Image Path' and
    a 'Zip File' column to the CSV metadata, representing the Zip file where
    the exam is stored and the path of each image file based on the provided
    ADNI Zip file.

    Parameters:
    - metadata_csv (pandas.DataFrame): The original metadata in the form
        of a DataFrame.
    - adni_file_path (str): The path to the ADNI exams Zip file.
    - args, kwargs: Additional arguments to pass to the superclass constructor.

    Example Usage:
    ```
    from adni_metadata_augmenter_operator import AdniMetadataAugmenterOperator

    # Create an instance of the AdniMetadataAugmenterOperator
    csv_file = read_csv_task()
    files_processor = AdniMetadataAugmenterOperator.partial(
        task_id='files_processor_task',
        metadata_csv=csv_file,
    ).expand(adni_file_path=list_files.output)

    ```

    Inherits from:
        - ZipFileProcessorOperator

    Template Fields:
        - metadata_csv (pandas.DataFrame): Template field to dynamically
            generate tasks.

    Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
    Date: 2024-02-20
    """

    template_fields = ('metadata_csv',)

    def __init__(
            self,
            metadata_csv,
            adni_file_path,
            destination_path=None,
            *args,
            **kwargs
    ):
        """
        Initialize the AdniMetadataAugmenterOperator.

        Parameters:
        - metadata_csv (pandas.DataFrame): The original metadata in the form
            of a DataFrame.
        - adni_file_path (str): The path to the ADNI Zip file.
        - destination_path (str, optional): The destination path for
            augmented files.
        - args, kwargs: Additional arguments to pass to the superclass
            constructor.
        """
        super().__init__(
            adni_file_path=adni_file_path,
            destination_path=destination_path,
            *args, **kwargs
        )
        self.metadata_csv = metadata_csv

    def _process(self):
        self.augmented_csv = self.metadata_csv.copy()

        files_path = os.path.join(self.destination_path, 'ADNI')
        self.augmented_csv["image_path"] = self.augmented_csv.apply(
            partial(path_from_csv_row, base_path=files_path), axis=1
        )
        names = self.augmented_csv.columns.tolist()
        names[names.index('image_path')] = 'Image Path'
        self.augmented_csv.columns = names

        rows_with_paths = self.augmented_csv['Image Path'].notnull()
        self.augmented_csv.loc[rows_with_paths, 'Zip File'] = self.file_path

    def _result(self):
        return self.augmented_csv
