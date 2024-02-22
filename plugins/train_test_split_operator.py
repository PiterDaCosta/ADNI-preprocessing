"""
Airflow Operator for Splitting Metadata CSV into Train and Test Sets.

This module defines an Apache Airflow operator that splits a metadata CSV into
train and test sets using scikit-learn's train_test_split function.
The operator is designed to be used within Apache Airflow workflows.

Operator:
- TrainTestSplitOperator: Operator for splitting metadata CSV into train
and test sets.


Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
from airflow.models import BaseOperator
from sklearn.model_selection import train_test_split


class TrainTestSplitOperator(BaseOperator):
    """
    Airflow Operator for Splitting Metadata CSV into Train and Test Sets.

    This operator splits a metadata CSV into train and test sets using
    scikit-learn's train_test_split function. It is designed to be used within
    Apache Airflow workflows.

    Parameters:
        metadata_csv (pd.DataFrame): The metadata CSV to be split.
        adni_files_list (list): List of ADNI files to filter the metadata CSV.
        ratio (float, optional): The ratio of the data to be used for training.
            Defaults to 0.8.
        args, kwargs: Additional arguments to pass to the superclass
            constructor.

    Example Usage:
    ```
    from train_test_split_operator import TrainTestSplitOperator

    # Create an instance of the TrainTestSplitOperator
    split_operator = TrainTestSplitOperator(
        task_id='split_train_test',
        metadata_csv=metadata_df,
        adni_files_list=['ADNI001.zip', 'ADNI002.zip'],
        ratio=0.8,
    )

    # Other relevant tasks and dependencies...
    ```

    Methods:
    - execute(context): Implementation of the train-test split logic.

    Inherits from:
    - BaseOperator

    Template Fields:
        metadata_csv (pd.DataFrame): Template field for dynamically generating
            tasks.
        adni_files_list (list): Template field for dynamically generating
            tasks.
    """

    template_fields = ('metadata_csv', 'adni_files_list')

    def __init__(
            self,
            metadata_csv,
            adni_files_list,
            ratio=0.8,
            *args,
            **kwargs
    ):
        """
        Initialize the TrainTestSplitOperator.

        Parameters:
            metadata_csv (pd.DataFrame): The metadata CSV to be split.
            adni_files_list (list): List of ADNI Zip files to filter the
                metadata CSV.
            ratio (float, optional): The ratio of the data to be used for
                training. Defaults to 0.8.
            args, kwargs: Additional arguments to pass to the superclass
                constructor.
        """
        super().__init__(*args, **kwargs)
        self.adni_files_list = adni_files_list
        self.ratio = ratio
        self.metadata_csv = metadata_csv

    def execute(self, context):
        """
        Execute the TrainTestSplitOperator.

        This method splits the metadata CSV into train and test sets using
        scikit-learn's train_test_split function.

        Parameters:
            context: The execution context provided by Airflow.

        Returns:
            dict: Dictionary containing 'train_csv' and 'test_csv'.
        """
        filtered_csv = self.metadata_csv[
            self.metadata_csv["Zip File"].isin(self.adni_files_list)
        ]
        [train_csv, test_csv] = train_test_split(
            filtered_csv,
            train_size=self.ratio
        )
        return {"train_csv": train_csv, "test_csv": test_csv}
