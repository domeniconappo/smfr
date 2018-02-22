FROM python:3.6-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}
ENV INSTALL_PATH /rest_server

RUN apk add --no-cache mariadb-dev g++ bash

RUN mkdir -p $INSTALL_PATH && mkdir -p $INSTALL_PATH/client && chmod a+w -R $INSTALL_PATH/client && mkdir -p /scripts && mkdir -p /configuration

WORKDIR $INSTALL_PATH

COPY ./rest_server/requirements.txt ./
COPY ./rest_server/src/. ./
COPY ./client/. ./client/
COPY ./rest_server/scripts/. /scripts/
COPY ./rest_server/src/config/. /configuration/

RUN pip install -U pip && pip install cassandra-driver && pip install --no-cache-dir -r requirements.txt
RUN apk del g++ mariadb-dev && apk add --no-cache mariadb-client-libs

RUN ["chmod", "a+x", "/scripts/wait_for_it.sh"]

EXPOSE 5555
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandra:9042"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5555", "--workers", "1", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "restserver", "start:app"]