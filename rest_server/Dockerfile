FROM python:3.6-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

RUN apk add --no-cache mariadb-dev g++ bash

ENV INSTALL_PATH /rest_server
RUN mkdir -p $INSTALL_PATH
RUN mkdir -p /scripts
RUN mkdir -p /configuration

WORKDIR $INSTALL_PATH

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN apk del g++ mariadb-dev && apk add --no-cache mariadb-client-libs
COPY ./src/. ./
COPY ./scripts/. /scripts/
COPY ./src/config/. /configuration/
RUN chmod a+x /scripts/*.sh
EXPOSE 5555
CMD ["/bin/bash", "/scripts/start.sh"]
ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}