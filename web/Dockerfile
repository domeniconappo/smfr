FROM python:3.6-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>
ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}

RUN apk add --no-cache g++ bash

ENV INSTALL_PATH /web
RUN mkdir -p $INSTALL_PATH
RUN mkdir -p $INSTALL_PATH/client

WORKDIR $INSTALL_PATH

COPY ./web/requirements.txt ./
RUN pip install -U pip
RUN pip install -r requirements.txt

COPY ./web/src/. ./
COPY ./client/. ./client/
COPY ./rest_server/src/config/config.yaml.* ./client/config/
COPY ./web/scripts/. /scripts/
RUN chmod a+x -R /scripts/

EXPOSE 8888
CMD ["/scripts/start.sh"]
