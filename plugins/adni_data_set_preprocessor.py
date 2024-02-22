"""
AdniDataSetPreprocessor Operator Class for ADNI exams preprocessing.

This class preprocess exams downloaded from ADNI and stores them
on an HDF5 file.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
from utils.adni_csv_helpers import preprocess_row, add_to_h5
from zip_file_processor_operator import ZipFileProcessorOperator


class AdniDataSetPreprocessor(ZipFileProcessorOperator):
    """
    Custom operator for preprocessing ADNI datasets.

    This operator extends the ZipFileProcessorOperator to handle the
    preprocessing of ADNI datasets. It processes each row of the dataset,
    applying converting each exam to nii format and standardizing it, then the
    result is stored in the HDF5 file specified by the 'dst_file_path'.

    Parameters:
    - adni_data_set_data (dict): Dictionary containing information about the
        dataset.
        - 'data_set': DataFrame, the dataset to be processed.
        - 'src_file_path': str, source exams Zip file path.
        - 'dst_file_path': str, file path for the HDF5 file where the results
            should be stored.

    Example usage:
    ```
    data_sets_with_files = create_hdf5_files(split_train_test.output)

    data_sets_processor = AdniDataSetPreprocessor.partial(
        task_id='adni_data_set_preprocessor',
    ).expand(adni_data_set_data=data_sets_with_files)
    ```

    Inherits from:
        - ZipFileProcessorOperator

    Template Fields:
        - adni_data_set_data (dict): Template field to dynamically
            generate tasks.

    Methods:
        - _process(): Implementation of the dataset preprocessing logic.

    Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
    Date: 2024-02-22
    """

    template_fields = ('adni_data_set_data', )

    def __init__(self, adni_data_set_data, *args, **kwargs):
        """
        Initialize the AdniDataSetPreprocessor.

        Parameters:
        - adni_data_set_data (dict): Dictionary containing information about
            the dataset.
        - args, kwargs: Additional arguments to pass to the superclass
            constructor.
        """
        super().__init__(
            adni_file_path=adni_data_set_data["src_file_path"],
            *args,
            **kwargs
        )
        self.adni_data_set_data = adni_data_set_data

    def _process(self):
        data_set = self.adni_data_set_data["data_set"]
        for i in range(data_set.shape[0]):
            row = data_set.iloc[i]
            preprocess_row(row)
            add_to_h5(row, self.adni_data_set_data["dst_file_path"])
