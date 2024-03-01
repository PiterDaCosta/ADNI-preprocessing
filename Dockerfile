FROM apache/airflow:2.8.1
COPY requirements.txt /
COPY requirements-test.txt /

ARG TEST_ENV="False"

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt


USER root

RUN if [ "$TEST_ENV" = "True" ]; then \
    sudo pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt; \
    sudo pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements-test.txt; \
  fi

# Installing Dcm2niix.
## Install Dependencies.

RUN apt-get update && apt-get upgrade -y && \
	apt-get install -y build-essential pkg-config cmake git pigz && \
	apt-get clean -y && apt-get autoclean -y && apt-get autoremove -y

## Get dcm2niix from github and compile it.
RUN cd /tmp && \
    rm -Rf dcm2niix && \
	git clone https://github.com/rordenlab/dcm2niix.git && \
	cd dcm2niix && mkdir build && cd build && \
	cmake -DBATCH_VERSION=ON -DUSE_OPENJPEG=ON .. && \
	make && make install


# Installing i2nii.
RUN apt-get install -y unzip && \
    cd /tmp && \
    curl -fLO https://github.com/rordenlab/i2nii/releases/latest/download/i2nii_lnx.zip && \
    unzip -o i2nii_lnx.zip && \
    cp -f i2nii /usr/local/bin/
