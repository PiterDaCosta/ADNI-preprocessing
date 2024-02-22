"""
Airflow Operator for Aggregating Augmented CSV Files.

This module defines an Apache Airflow operator for aggregating augmented CSV
files (see adni_metadata_augmenter_operator.py).
The operator takes an original CSV file, a list of augmented CSV files, and
aggregates them into a single CSV file with added 'Zip File' and 'Image Path'
columns.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-20
"""
import pandas as pd
from airflow.models import BaseOperator


class AugmentedCsvAggregatorOperator(BaseOperator):
    """
    Apache Airflow Operator for Aggregating Augmented CSV Files.

    This operator aggregates augmented CSV files into a single CSV file,
    updating the 'Zip File' and 'Image Path' columns in the original CSV.
    It is designed to be used within Apache Airflow workflows.

    Parameters:
    - original_csv_path (str): The path to the original CSV file.
    - augmented_csv_files (list): List of DataFrames representing augmented
        CSV files.
    - destination_csv_path (str): The path to the destination CSV file.
    - args, kwargs: Additional arguments to pass to the superclass constructor.

    Example Usage:
    ```
    from augmented_csv_aggregator_operator import AugmentedCsvAggregatorOperato

    # Create an instance of the AugmentedCsvAggregatorOperator
    aggregator = AugmentedCsvAggregatorOperator(
        task_id='augmented_csv_aggregator',
        original_csv_path='/path/to/original.csv',
        augmented_csv_files=[df1, df2, df3],
        destination_csv_path='/path/to/aggregated.csv',
    )

    # Other relevant tasks and dependencies...
    ```

    Methods:
    - execute(context): Implementation of the aggregation logic.

    Inherits from:
    - BaseOperator

    Template Fields:
    - augmented_csv_files (list): Template field to dynamically generate tasks.
    """

    template_fields = ('augmented_csv_files',)

    def __init__(
        self,
        original_csv_path,
        augmented_csv_files,
        destination_csv_path,
        *args,
        **kwargs
    ):
        """
        Initialize the AugmentedCsvAggregatorOperator.

        Parameters:
        - original_csv_path (str): The path to the original CSV file.
        - augmented_csv_files (list): List of DataFrames representing augmented
            CSV files.
        - destination_csv_path (str): The path to the destination CSV file.
        - args, kwargs: Additional arguments to pass to the superclass
            constructor.
        """
        super(self.__class__, self).__init__(
            *args,
            **kwargs
        )
        self.original_csv_path = original_csv_path
        self.augmented_csv_files = augmented_csv_files
        self.destination_csv_path = destination_csv_path

    def execute(self, context):
        """
        Execute the AugmentedCsvAggregatorOperator.

        This method aggregates the augmented CSV files into a single CSV file,
        updating the 'Zip File' and 'Image Path' columns in the original CSV.

        Parameters:
        - context: The execution context provided by Airflow.

        Returns:
        None
        """
        self.log.debug("AugmentedCsvAggregator execute")
        aggregated = pd.read_csv(self.original_csv_path)
        aggregated['Zip File'] = None
        aggregated['Image Path'] = None
        for df in self.augmented_csv_files:
            rows_with_paths = df['Image Path'].notnull()
            aggregated.update(df.loc[rows_with_paths])

        aggregated.to_csv(self.destination_csv_path, index=False)
