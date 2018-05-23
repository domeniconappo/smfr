FROM efas/smfr_base:${IMAGE_TAG}
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV LANG=C.UTF-8

ENV INSTALL_PATH /web
RUN mkdir -p $INSTALL_PATH

WORKDIR $INSTALL_PATH

COPY ./web/requirements.txt ./

ARG http_proxy
ARG https_proxy

RUN pip install -U pip && pip install -r requirements.txt

COPY ./web/src/. ./
COPY ./web/scripts/. /scripts/
RUN chmod a+x -R /scripts/

EXPOSE 8888
CMD ["/scripts/start.sh"]
