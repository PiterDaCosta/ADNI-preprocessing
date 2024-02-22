"""
Zip File Processor Operator Module.

This module defines a base operator, ZipFileProcessorOperator, for processing
ZIP files within Apache Airflow workflows. It includes methods for
uncompressing, processing, and cleaning up extracted files. The operator is
designed to be subclassed to implement custom processing logic.


Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
import abc
import os
import shutil

from airflow.models import BaseOperator
from zipfile import ZipFile


class ZipFileProcessorOperator(BaseOperator):
    """
    Base Operator for Processing ZIP Files in Apache Airflow Workflows.

    This operator provides a base class for processing ADNI exams ZIP files
    within Apache Airflow workflows. It includes methods for uncompressing,
    processing, and cleaning up extracted files.

    Parameters:
        adni_file_path (str): Path to the ZIP file to be processed.
        destination_path (str, optional): Destination path for extracting
            files.
            Defaults to "shared/temporary/extracted/{filename}".

    Methods:
    - execute(context): Executes the operator, including uncompressing,
        processing, and cleaning up.
    - _cleanup(): Cleans up the extracted files and directory.
    - _process(): Abstract method for processing the extracted files
        (to be implemented by subclasses).
    - _result(): Abstract method to return the result of processing
        (to be implemented by subclasses).
    - _uncompress(): Uncompresses the specified ZIP file to the
        destination path.

    Attributes:
    - return_value: Result of processing, set by the _result() method.
    """

    def __init__(self, adni_file_path, destination_path=None, *args, **kwargs):
        """
        Initialize the ZipFileProcessorOperator.

        Parameters:
            adni_file_path (str): Path to the ZIP file to be processed.
            destination_path (str, optional): Destination path for extracting
                files.
                Defaults to "shared/temporary/extracted/{filename}".
            args, kwargs: Additional arguments to pass to the superclass
                constructor.
        """
        super().__init__(*args, **kwargs)
        self.return_value = None
        self.file_path = adni_file_path
        self.adni_file_filename = os.path.basename(adni_file_path)

        if destination_path is None:
            # TODO: Extract this from environment variables.
            self.destination_path = os.path.join(
                "shared",
                "temporary",
                "extracted",
                os.path.splitext(self.adni_file_filename)[0]
            )
        else:
            self.destination_path = destination_path

    def execute(self, context):
        """
        Execute the ZipFileProcessorOperator.

        This method executes the operator, including uncompressing, processing,
        and cleaning up.

        Parameters:
            context: The execution context provided by Airflow.
        """
        self._uncompress()
        self._process()
        self._cleanup()
        return self._result()

    def _cleanup(self):
        shutil.rmtree(self.destination_path)

    @abc.abstractmethod
    def _process(self):
        """
        Abstract method for processing the extracted files.

        This method should be implemented by subclasses to define the
        processing logic.
        """
        pass

    def _result(self):
        return None

    def _uncompress(self):
        self.log.info(
            "Preparing to uncompress %s",
            self.adni_file_filename
        )

        if not os.path.exists(self.destination_path):
            os.makedirs(self.destination_path)
        with ZipFile(self.file_path, 'r') as zObject:
            zObject.extractall(path=self.destination_path)
