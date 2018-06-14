FROM python:3.6
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ARG http_proxy
ARG https_proxy

RUN apt-get update && apt-get install -y software-properties-common apt-file apt-utils
RUN apt-file update

RUN apt-get install -y liblapack-dev liblapack-pic liblapack3 liblapack-test  \
    liblapacke liblapacke-dev hdf5-tools libhdf5-serial-dev gfortran g++ wget \
    libssl-dev openssl

COPY requirements.txt /
RUN pip3.6 install -U pip && pip3.6 install -r requirements.txt

# Clean up APT
RUN apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
