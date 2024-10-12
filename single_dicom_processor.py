from airflow.models import Variable
import pandas as pd
import logging
import h5py

from utils.adni_images_processing import get_processor


def create_hdf5_file(file_path):
    h5 = h5py.File(file_path, 'w')
    string_dt = h5py.special_dtype(vlen=str)
    h5.create_dataset(
        'ID',
        shape=(0,),
        maxshape=(None,),
        dtype=string_dt
    )
    h5.create_dataset(
        'X_nii',
        shape=(0, 100, 100, 120),
        maxshape=(None, 100, 100, 120),
        chunks=None
    )
    h5.create_dataset('X_Age', shape=(0,), maxshape=(None,))
    h5.create_dataset('X_Sex', shape=(0,), maxshape=(None,))
    h5.create_dataset('y', shape=(0,), maxshape=(None,))
    h5.close()


def setup():
    Variable.set('adni_dcm2niix_path', '/usr/bin/dcm2niix')
    Variable.set('adni_brain_template', 'shared/atlas/152mni/pet_ref_registered.nii')
    Variable.set('adni_mask_template', 'shared/atlas/152mni/mni_icbm152_t1_tal_nlin_sym_09c_mask.nii')

    Variable.set('adni_working_directory', '.')
    # P02
    Variable.set('adni_preprocess_config', [
        {"format_standardizer": {}},
        {"slice_squisher": {}},
        {"ants_image_converter": {}},
        {"apply_registration": {}},
        {"normalize": {}},
        {"apply_mask": {}},
        {"resample": {
            "dims": {"x": 100, "y": 120, "z": 100}
        }},
        {"hdf5_storer": {}}
    ], serialize_json=True)


def preprocess_dicom(dicom_path, age, sex, destination_path):
    setup()
    log = logging.getLogger(__name__)

    preproc_config = Variable.get(
        'adni_preprocess_config',
        deserialize_json=True
    )
    create_hdf5_file(destination_path)
    adni_data_set_data = {
        'dst_file_path': destination_path
    }
    target = None

    row = pd.Series({
        "Image Data ID": "1",
        "Image Path": dicom_path,
        "Format": "DCM",
        "Age": age,
        "Sex": sex,
        "Group": "C",
    })

    for step_data in preproc_config:
        processor_name = list(step_data.keys())[0]
        processor_kwargs = list(step_data.values())[0]

        log.info(f"Step: {processor_name}")
        processor = get_processor(processor_name)
        if processor is None:
            raise Exception(
                f"Step processor not found for '{processor_name}'."
            )
        target = processor(
            row,
            target=target,
            data_set_data=adni_data_set_data,
            **processor_kwargs
        )


if __name__ == "__main__":
    age = 60
    sex = 'F'
    # dicom_path = "./tests/test_data/PET_EXAMPLE/ADNI/301_S_6811/ANONYMIZED_FDG/2019-12-05_14_50_38.0/I1265032"
    dicom_path = "./tests/test_data/PET_EXAMPLE/ADNI/941_S_1004/ADNI_Brain_PET__Raw/2018-01-30_12_28_14.0/I1000004"
    destination_path = './subject-dicom.hdf5'
    preprocess_dicom(dicom_path, age, sex, destination_path)
