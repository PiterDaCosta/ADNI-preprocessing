"""
Test module for chainable image processor functions and related functions.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-26
"""
import os
from plugins.utils.adni_images_processing import I2NiixConversor


def test_I2NiixConversor_convert(hrrt_dir_path):
    """
    Test the execution of the I2NiixConversor convert method.

    This tests checks that the source image is correctly converted and that
    no temporary files are keep after the process finishes.
    """
    conversor = I2NiixConversor(
        image_file_path=hrrt_dir_path,
        image_type='HRRT',
        image_metadata={'Image Data ID': '1'},
    )
    result = conversor.convert()
    assert result["image"].size > 0
    assert not os.path.exists(conversor.temp_file_name)
