FROM python:3.5-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}

RUN apk add --no-cache mariadb-dev g++ bash

ENV INSTALL_PATH /rest_server
RUN mkdir -p $INSTALL_PATH
RUN mkdir -p $INSTALL_PATH/client
RUN chmod g+w -R $INSTALL_PATH/client
RUN mkdir -p /scripts
RUN mkdir -p /configuration

WORKDIR $INSTALL_PATH

COPY ./rest_server/requirements.txt ./
RUN pip install -U pip
RUN pip install --no-cache-dir -r requirements.txt
RUN apk del g++ mariadb-dev && apk add --no-cache mariadb-client-libs

COPY ./rest_server/src/. ./
COPY ./client/. ./client/
COPY ./rest_server/src/errors.py ./client/

COPY ./rest_server/scripts/. /scripts/
COPY ./rest_server/src/config/. /configuration/
RUN chmod a+x /scripts/*.sh
EXPOSE 5555
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandra:9042"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5555", "--workers", "1", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "restserver", "start:app"]