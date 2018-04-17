FROM python:3.6-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${http_proxy}
ENV CASSANDRA_KEYSPACE ${cassandra_keyspace}
ENV CASSANDRA_HOST ${cassandra_host}
ENV INSTALL_PATH /annotator
ENV LANG=C.UTF-8

RUN apk add --no-cache --upgrade apk-tools --force --repository http://dl-3.alpinelinux.org/alpine/edge/main/ --allow-untrusted
RUN apk add --no-cache --force-broken-world --force openblas openblas-dev  --repository http://dl-3.alpinelinux.org/alpine/edge/community/ --allow-untrusted
RUN apk add --no-cache gcc musl-dev bash
RUN pip3.6 install -U pip
RUN pip3.6 install cassandra-driver

RUN mkdir -p $INSTALL_PATH && mkdir -p /scripts && mkdir -p /smfr_core
WORKDIR $INSTALL_PATH
COPY ./annotator/requirements.txt ./

RUN pip3.6 install -r requirements.txt

WORKDIR /smfr_core
RUN python3.6 setup.py install

COPY ./annotator/src/. ./
COPY ./annotator/scripts/. /scripts/
COPY ./smfr_core/. /smfr_core/

WORKDIR $INSTALL_PATH
EXPOSE 5556

RUN ["chmod", "a+x", "/scripts/wait_for_it.sh"]
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandra:9042"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5556", "--workers", "1", "--log-level", "warning", "--timeout", "240", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "annotator", "start:app"]
