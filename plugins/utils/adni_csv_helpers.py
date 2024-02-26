"""
Helper functions to be used when processing ADNI metadat CSV files.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-26
"""
import os
import re
from datetime import datetime
from airflow.models import Variable


def _convert_to_dir_name(str):
    """
    Convert the input string to a valid directory name.

    Convert the input string to a valid directory name by replacing
    non-alphanumeric characters with underscores.

    Parameters:
    - s (str): Input string to be converted.

    Returns:
    str: Converted directory name.
    """
    return re.sub(r"[^0-9a-zA-Z.+#-]", "_", str)


def path_from_csv_row(row, base_path):
    """
    Generate a path based on the info. provided in a CSV row and a base path.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - base_path (str): The base path where the generated path will be
        constructed.

    Returns:
    str or None: The constructed path if found; otherwise, None.
    """
    # There might be several exams of this type for this subject.
    path = os.path.join(
        base_path,
        row["Subject"],
        _convert_to_dir_name(row["Description"])
    )
    # Get the folder whose name starts with row["Acq Date"].
    # The date is in the format MM/DD/YYYY but the directory is in
    # 'year-month-day_hour_minute_second.millisecond'.
    date = datetime.strptime(
        row["Acq Date"],
        "%m/%d/%Y"
    ).strftime("%Y-%m-%d")
    found = False
    if os.path.isdir(path):
        for folder in os.listdir(path):
            if folder.startswith(date):
                if row["Format"] == "DCM" or row["Format"] == "HRRT":
                    path = os.path.join(path, folder, row["Image Data ID"])
                    if os.path.isdir(path):
                        found = True
                    else:
                        break
                elif row["Format"] == "ECAT" or row["Format"] == "NIFTY":
                    # TODO: Check if 'NIFTY' is the way this appears in the csv
                    path = os.path.join(path, folder)
                    # Inside this folder, there's a folder with the series ID
                    # (I think it should always be just one).
                    path = os.path.join(path, os.listdir(path)[0])
                    if row["Format"] == "ECAT":
                        ext = "v"
                    else:
                        ext = "nii"
                    for file in os.listdir(path):
                        if file.endswith("_{}.{}".format(
                                row["Image Data ID"],
                                ext)
                        ):
                            path = os.path.join(path, file)
                            found = True
                            break
                else:
                    # TODO: Complete for other formats.
                    raise NotImplementedError(
                        "Image Data ID not found in path: {}".format(path)
                    )
                break
    if not found:
        return None
    return path


def _preprocessed_image_path(row):
    """
    Generate the destination path for a preprocessed image.

    Generate the destination path for a preprocessed image based on the input
    row's information.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.

    Returns:
    str: The destination path for the preprocessed image.
    Raises:
    NotImplementedError: If the specified image format is not implemented.
    """
    src_path = row["Image Path"]
    if row["Format"] in ['ECAT', 'HRRT', 'DCM']:
        _p, file = os.path.split(src_path)
        filename = file + ".nii"
        dst_path = os.path.join(
            Variable.get('adni_working_directory'),
            filename
        )
    else:
        raise NotImplementedError(f"Format {row['Format']} not implemented.")
    return dst_path
