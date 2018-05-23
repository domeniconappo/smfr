FROM efas/smfr_base:${IMAGE_TAG}
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>


ENV INSTALL_PATH /persister
ENV LANG=C.UTF-8

RUN mkdir -p $INSTALL_PATH && mkdir -p /scripts && mkdir -p /config
WORKDIR $INSTALL_PATH

COPY ./persister/src/. ./
COPY ./persister/scripts/. /scripts/

RUN ["chmod", "a+x", "/scripts/wait_for_it.sh"]
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandrasmfr:9160"]
CMD ["--", "python3.6", "start.py"]
