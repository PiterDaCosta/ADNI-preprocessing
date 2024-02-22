"""
Airflow Operator for Listing PET Zip Files in a Folder.

This module defines an Apache Airflow operator that lists PET zip files in a
specified folder.
The operator is designed to be used within Apache Airflow workflows.

Operator:
- ListZipFilesOperator: Operator for listing PET zip files in a folder.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
import os
import re
from airflow.models import BaseOperator
from pathlib import Path


class ListZipFilesOperator(BaseOperator):
    """
    Apache Airflow Operator for Listing PET Zip Files in a Folder.

    This operator lists PET zip files in a specified folder and returns the
    file list. It is designed to be used within Apache Airflow workflows.

    Example Usage:
    ```
    from operators.list_zip_files_operator import ListZipFilesOperator

    # Create an instance of the ListZipFilesOperator
    zip_file_list_operator = ListZipFilesOperator(
        task_id='list_files',
        folder_path='/path/to/pet/files'
    )

    # Other relevant tasks and dependencies...
    ```

    Methods:
    - execute(context): Implementation of the file listing logic.

    Inherits from:
    - BaseOperator

    Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
    Date: 2024-02-22
    """

    def __init__(self, folder_path,  *args, **kwargs):
        """
        Initialize the ListZipFilesOperator.

        Parameters:
            folder_path (str): The path to the folder containing PET zip files.
            args, kwargs: Additional arguments to pass to the superclass
                constructor.
        """
        super().__init__(*args, **kwargs)
        self.folder_path = folder_path

    def execute(self, context):
        """
        Execute the ListZipFilesOperator.

        This method lists PET zip files in the specified folder and returns
        the file list.

        Parameters:
            context: The execution context provided by Airflow.

        Returns:
            list: List of paths to PET zip files.
        """
        folder_path = Path(self.folder_path)
        if folder_path.is_dir():
            file_list = [
                os.path.join(folder_path, file)
                for file in os.listdir(folder_path)
                if re.match(r'PET[0-9]*\.zip', file)
            ]
        else:
            raise FileNotFoundError(
                f"Local folder not found: {self.folder_path}"
            )
        self.log.info("Files in folder %s: %s", self.folder_path, file_list)
        return file_list
