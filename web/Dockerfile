FROM python:3.6-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>
ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}

RUN apk add --no-cache g++ bash python3-dev libev-dev libev

ENV INSTALL_PATH /web
RUN mkdir -p $INSTALL_PATH && mkdir -p /smfr_core

WORKDIR $INSTALL_PATH

COPY ./web/requirements.txt ./
RUN pip install -U pip && pip install -r requirements.txt

COPY ./smfr_core/. /smfr_core/
WORKDIR /smfr_core
RUN python3.6 setup.py build && python3.6 setup.py install

COPY ./web/src/. ./
COPY ./web/scripts/. /scripts/
RUN chmod a+x -R /scripts/

EXPOSE 8888
CMD ["/scripts/start.sh"]
