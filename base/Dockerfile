FROM d-registry.jrc.it:5000/public/python:3.6.6
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ARG http_proxy
ARG https_proxy

RUN apt-get update && apt-get install -y software-properties-common apt-file apt-utils
RUN apt-file update

RUN apt-get install -y liblapack-dev liblapack-pic liblapack3 liblapack-test  \
    liblapacke liblapacke-dev hdf5-tools libhdf5-serial-dev gfortran g++ wget \
    libssl-dev openssl
RUN pip3.6 install -U pip && pip3.6 install cassandra-driver kafka-python "numpy<1.15.0" cython tensorflow

COPY requirements.txt /
RUN pip3.6 install -r requirements.txt && apt-get autoremove -y \
 && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
# TODO https://software.intel.com/en-us/articles/intel-optimization-for-tensorflow-installation-guide
