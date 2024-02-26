"""
ADNI exam files processors and helpers.

Chainable image processor and helper functions to be used when
processing raw ADNI exam files.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-26
"""
import abc
import ants
import h5py
import numpy as np
import os
import re
import shutil
import subprocess
import uuid
from airflow.models import Variable
from pathlib import Path
import nibabel as nib

PROCESSORS = {}


def register_processor(processor_name):
    """
    Register an image file chainable processor (decorator function).

    Parameters:
    - processor_name (str): The name of the processor to register.

    Returns:
    callable: Decorator function.
    """
    def decorator(fn):
        PROCESSORS[processor_name] = fn
        return fn
    return decorator


def get_processor(processor_name):
    """
    Retrieve an image processor function.

    Parameters:
    - processor_name (str): The name of the processor to retrieve.

    Returns:
    callable or None: The processor function if found; otherwise, None.
    """
    return PROCESSORS.get(processor_name)


class ToNpNiftyConversor:
    """
    Abstract base class for converters to NIfTI format.

    Attributes:
    - image_file_path (str): Path to the input image file.
    - image_type (str): Type of the input image.

    Methods:
    - convert(): Public method to convert the image.
    - _convert_to_nifty(): Abstract method to perform the actual conversion.
    - _cleanup(): Method to perform cleanup after conversion.
    """

    def __init__(self, image_file_path, image_type):
        """
        Initialize the ToNpNiftyConversor.

        Parameters:
        - image_file_path (str): Path to the input image file.
        - image_type (str): Type of the input image.
        """
        self.image_file_path = image_file_path
        self.image_type = image_type

    def convert(self):
        """
        Convert the image to NIfTI format.

        Returns:
        dict: Dictionary containing the converted image and its affine
            transformation matrix.
        """
        converted = self._convert_to_nifty()
        self._cleanup()
        return converted

    @abc.abstractmethod
    def _convert_to_nifty(self):
        """
        Abstract method to perform the actual conversion.

        Returns:
        dict: Dictionary containing the converted image and its affine
            transformation matrix.
        """
        pass

    def _cleanup(self):
        """Clean up temporary files after conversion."""
        pass


class Dcm2NiixConversor(ToNpNiftyConversor):
    """
    Converter class for DICOM to NIfTI using dcm2niix.

    Methods:
    - _convert_to_nifty(): Convert DICOM to NIfTI using dcm2niix.
    - cleanup(): Cleanup temporary files generated during conversion.
    """

    def _convert_to_nifty(self):
        temp_file_name = str(uuid.uuid4())
        command = [Variable.get('adni_dcm2niix_path'), '-o',
                   Variable.get('adni_working_directory'), '-w', '1', '-f',
                   temp_file_name, self.image_file_path]
        subprocess.run(command)
        self.temp_file_name = temp_file_name + ".nii"

        self.temporal_nii_file = os.path.join(
            Variable.get('adni_working_directory'),
            self.temp_file_name
        )
        nifty = nib.load(self.temporal_nii_file)
        return {'image': nifty.get_fdata(), 'affine': nifty.affine}

    def _cleanup(self):
        if self.temporal_nii_file is not None:
            os.remove(self.temporal_nii_file)


class I2NiixConversor(ToNpNiftyConversor):
    """
    Converter class for I2NII (I2Niix) conversor to NIfTI.

    Methods:
    - _pick_hrrt_slide(hrrt_directory): Pick an HRRT slide from the
        specified directory.
    - _prepare_hrrt_files(): Prepare HRRT files for conversion.
    - _convert_to_nifty(): Convert I2NII format to NIfTI.
    - cleanup(): Cleanup temporary files generated during conversion.
    """

    def _pick_hrrt_slide(self, hrrt_directory):
        data_files = []
        for root, dirs, files in os.walk(hrrt_directory):
            for file in files:
                # There should be one .hdr for slide.
                if file.endswith('.i'):
                    # print(os.path.join(root, file)
                    data_files.append(file)
        data_files.sort()
        # We just pick one that's close to the end.
        return data_files[int(len(data_files) * 0.75)]

    def _prepare_hrrt_files(self):
        selected_slide = self._pick_hrrt_slide(self.image_file_path)
        dst_file_name = re.sub(
            r"_[A-Z0-9]+_[A-Z0-9]+\.i$",
            ".i", selected_slide
        )

        shutil.copyfile(
            os.path.join(self.image_file_path, selected_slide),
            os.path.join(Variable.get('adni_working_directory'), dst_file_name)
        )
        shutil.copyfile(
            os.path.join(self.image_file_path, selected_slide + ".hdr"),
            os.path.join(
                Variable.get('adni_working_directory'),
                dst_file_name + '.hdr'
            )
        )
        # Some .hdr files have the name of the data file like this:
        # 'name of data file := file name removed during de-identification'
        # This confuses the convertor, we need to change that.
        hdr_filename = os.path.join(
            Variable.get('adni_working_directory'),
            dst_file_name + '.hdr'
        )
        f = open(hdr_filename, 'r')
        filedata = f.read()
        f.close()
        new_data = filedata.replace(
            'name of data file := file name removed during de-identification',
            'name of data file := ' + dst_file_name
        )
        f = open(hdr_filename, 'w')
        f.write(new_data)
        f.close()
        return os.path.join(
            Variable.get('adni_working_directory'),
            dst_file_name + '.hdr'
        )

    def _convert_to_nifty(self):
        # I2NII convertor doesn't work well with the HRRT files,
        # this seems to be because there's something wrong on the .i.hdr,
        # the 'name of data file' key value points to a file that doesn't
        # exist, we think that's this is because the files were renamed at some
        # point and this value wasn't updated.
        # Also, the convertor only handle one slide at a time.
        # So what we do is to pick one of the middle layers, rename the files
        # and then convert THAT layer to .nii.
        if self.image_type == 'HRRT':
            self.image_file_path = self._prepare_hrrt_files()
        command = [
            Variable.get('adni_i2nii_path'), '-o',
            Variable.get('adni_working_directory'),
            self.image_file_path
        ]
        subprocess.run(command)

        file_path = re.sub(r"\.hdr$", "", self.image_file_path)
        self.temp_file_name = Path(file_path).stem + '.nii'

        self.temporal_nii_file = os.path.join(
            Variable.get('adni_working_directory'),
            self.temp_file_name
        )
        nifty = nib.load(self.temporal_nii_file)
        return {'image': nifty.get_fdata(), 'affine': nifty.affine}

    def _cleanup(self):
        # TODO: Clean up the files generated by I2NIIx on the temporary files
        # directory.
        if self.temporal_nii_file is not None:
            os.remove(self.temporal_nii_file)


@register_processor('format_standardizer')
def image_format_standarizer(row, **kwargs):
    """
    Processor function for standardizing image formats.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    dict: Dictionary containing the converted image and its affine
        transformation matrix.
    """
    if row["Format"] in ['DCM', 'ECAT']:
        conversor = Dcm2NiixConversor(
            image_file_path=row["Image Path"],
            image_type=row["Format"]
        )
    elif row["Format"] == 'HRRT':
        conversor = I2NiixConversor(
            image_file_path=row["Image Path"],
            image_type=row["Format"]
        )

    return conversor.convert()


@register_processor('slice_squisher')
def slice_squisher(row, **kwargs):
    """
    Processor function for squishing slices of an image.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    dict: Dictionary containing the processed image and its affine
        transformation matrix.
    """
    original = kwargs['target']['image']
    frames = original.shape[-1]
    if row['Format'] == 'ECAT':
        if len(original.shape) > 3 and frames > 1:
            original = original[..., int(frames * 0.75)]
    else:
        if len(original.shape) > 3 and frames > 1:
            start_index = 1  # Start index of the range
            # End index of the range (limited to frames)
            end_index = min(3, frames)
            # Slice the array to select the desired range along the
            # last dimension
            sliced_array = original[..., start_index:end_index]
            # Calculate the mean along the last dimension of the sliced array
            mean_result = np.mean(sliced_array, axis=-1)
            original = mean_result
    if row["Format"] == 'HRRT':
        original = np.flip(original)
    return {'image': original, 'affine': kwargs['target']['affine']}


@register_processor('ants_image_converter')
def convert_to_ants(row, **kwargs):
    """
    Processor function for converting images to ANTs format.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    ants.core.ants_image.ANTsImage: ANTs image object.
    """
    # There are some problems with the orientation,
    # Using nibabel to create a Nifty and then storing it to a temporary file
    # Seems to do the trick.
    temp_nii_file = str(uuid.uuid4()) + ".nii"
    new_nifti_img = nib.Nifti1Image(
        kwargs["target"]["image"],
        kwargs["target"]["affine"])
    nib.save(new_nifti_img, temp_nii_file)
    image = ants.image_read(temp_nii_file)
    os.remove(temp_nii_file)
    return image


@register_processor('apply_registration')
def apply_registration(row, **kwargs):
    """
    Processor function for applying image registration.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    ants.core.ants_image.ANTsImage: Registered ANTs image object.
    """
    fixed = ants.image_read(Variable.get('adni_brain_template'))
    reg12 = ants.registration(fixed, kwargs["target"], 'Affine')
    return reg12['warpedmovout']


@register_processor('apply_mask')
def apply_mask(row, **kwargs):
    """
    Processor function for applying a mask to an image.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    ants.core.ants_image.ANTsImage: Masked ANTs image object.
    """
    mask = ants.image_read(Variable.get('adni_mask_template'))
    return ants.mask_image(kwargs["target"], mask)


@register_processor('convert_to_numpy')
def convert_to_numpy(row, **kwargs):
    """
    Processor function for converting an ANTs image to a NumPy array.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    np.ndarray: NumPy array representing the image.
    """
    return kwargs["target"].numpy()


@register_processor('normalize')
def normalize(row, **kwargs):
    """
    Processor function for normalizing image intensity.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    ants.core.ants_image.ANTsImage: Normalized ANTs image object.
    """
    image_array = kwargs["target"]
    # Divide each pixel by the maximum value of the image to
    # normalize between 0 and 1
    image_max = np.max(image_array)
    first_normalization_image = image_array / image_max
    # Calculate avg and stdev to further normalize
    average_intensity = np.mean(first_normalization_image)
    stdev_intensity = np.std(first_normalization_image)
    normalized_image = ((first_normalization_image - average_intensity)
                        / stdev_intensity)
    return ants.from_numpy(normalized_image)


@register_processor("resample")
def resample(row, **kwargs):
    """
    Processor function for resampling an image.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    ants.core.ants_image.ANTsImage: Resampled ANTs image object.
    """
    image_array = kwargs["target"]
    dims = kwargs["dims"]
    return ants.resample_image(
        image_array,
        (dims["x"], dims["y"], dims["z"]),
        True,
        1
    )


@register_processor('hdf5_storer')
def hdf5_storer(row, **kwargs):
    """
    Processor function for storing image data in an HDF5 file.

    Parameters:
    - row (pandas.Series): A row from the ADNI metadata CSV file.
    - **kwargs: Additional keyword arguments.

    Returns:
    None
    """
    hdf5_file_path = kwargs["data_set_data"]["dst_file_path"]
    h5 = h5py.File(hdf5_file_path, 'a')
    h5['ID'].resize(h5['ID'].shape[0] + 1, axis=0)
    h5['X_nii'].resize(h5['X_nii'].shape[0] + 1, axis=0)
    h5['X_Age'].resize(h5['X_Age'].shape[0] + 1, axis=0)
    h5['X_Sex'].resize(h5['X_Sex'].shape[0] + 1, axis=0)
    h5['y'].resize(h5['y'].shape[0] + 1, axis=0)

    h5['ID'][-1:] = row["Image Data ID"]

    struct_arr = kwargs["target"].to_nibabel().get_fdata().astype(np.float32)

    # This two lines are specific for using the martindyrba model.
    # TODO: Move this to a different processor.
    struct_arr = np.transpose(struct_arr, (2, 0, 1))
    struct_arr = np.flip(struct_arr)

    h5['X_nii'][-1:] = struct_arr
    h5['X_Age'][-1:] = row["Age"]
    h5['X_Sex'][-1:] = 0 if (row["Sex"] == 'F') else 1

    if row["Group"] == 'AD':
        group = 0
    elif row["Group"] == 'MCI':
        group = 1
    elif row["Group"] == 'CN':
        group = 2
    else:
        # We consider all other labels as MCI.
        group = 1
    h5['y'][-1:] = group
    h5.close()
