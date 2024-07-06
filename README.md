[![Test status](https://github.com/PiterDaCosta/ADNI-preprocessing/actions/workflows/docker-image.yml/badge.svg)](https://github.com/PiterDaCosta/ADNI-preprocessing/actions/workflows/docker-image.yml)

# ADNI-preprocessing
ETL system to be used for converting ADNI files to HDF5 ready to be used on ML projects

# Running
The first time that the project is executed run:
```
docker compose build
```
This will prepare the containers 

And then run:
```commandline
docker compose up
```
to run the project.
