"""
Test module for ListZipFilesOperator operator.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
from list_zip_files_operator import ListZipFilesOperator
from pathlib import Path
from plugins.utils.testing_utils import dagrun_setup


def test_list_zip_files_execute(dag, tmpdir):
    """
    Test the execution of the ListZipFilesOperator operator.

    This tests checks id the new columns are populated correctly.
    """
    data_dir = Path(tmpdir) / "data"
    data_dir.mkdir()

    file_path = data_dir / "file_that_should_be_ignored.txt"
    file_path.touch()

    for i in range(3):
        file_path = data_dir / "PET{i}.zip".format(i=i)
        file_path.touch()

    task_id = 'test_task'
    ListZipFilesOperator(dag=dag, task_id=task_id, folder_path=str(data_dir))
    result = dagrun_setup(dag, task_id)
    msg = ("Directory only contains 3 zip files but "
           "{result_len} was returned").format(result_len=len(result))
    assert len(result) == 3, msg
