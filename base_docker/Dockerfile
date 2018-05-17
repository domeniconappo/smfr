FROM python:3.6
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${http_proxy}

RUN apt-get update
RUN apt-get install -y software-properties-common apt-file apt-utils python-software-properties
RUN apt-file update

RUN apt-get install -y liblapack-dev liblapack-pic liblapack3 liblapack-test  \
    liblapacke liblapacke-dev hdf5-tools libhdf5-serial-dev gfortran g++ wget \
    libssl-dev openssl

RUN mkdir -p /smfr_core/
COPY smfr_core/requirements.txt /smfr_core/
RUN pip3.6 install -U pip && \
    pip3.6 install -r /smfr_core/requirements.txt

COPY ./smfr_core/. /smfr_core/

RUN pip3.6 install /smfr_core/
