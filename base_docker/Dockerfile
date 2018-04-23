FROM python:3.6-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${http_proxy}

RUN apk add --no-cache g++ bash python3-dev libev-dev libev
RUN pip3.6 install -U pip && \
    pip3.6 install cassandra-driver flask ujson gunicorn numpy cython kafka-python tensorflow