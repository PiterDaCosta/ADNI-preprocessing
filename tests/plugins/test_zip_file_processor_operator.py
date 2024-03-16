"""
Test module for ZipFileProcessorOperator operator.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
import os
from zip_file_processor_operator import ZipFileProcessorOperator


def _assert_process(self):
    dst_path = os.path.join(self.destination_path, "ADNI")
    exams_folders = [
        f for f
        in os.listdir(dst_path)
        if os.path.isdir(os.path.join(dst_path, f))
    ]
    assert len(exams_folders) == 2


def test_uncompression(monkeypatch, pets_zip_file_path, tmp_path):
    """
    Test the execution of the ZipFileProcessor operator.

    This tests checks that the file is correctly uncompressed and that
    it's cleaned after the process finishes.
    """
    monkeypatch.setattr(
        ZipFileProcessorOperator, "__abstractmethods__", set()
    )
    monkeypatch.setattr(
        ZipFileProcessorOperator, "_process", _assert_process
    )
    operator = ZipFileProcessorOperator(
        task_id='test_task',
        adni_file_path=pets_zip_file_path,
        destination_path=tmp_path
    )
    operator.execute({})

    # Checking that the cleanup process was correctly executed.
    assert not os.path.isdir(operator.destination_path)
