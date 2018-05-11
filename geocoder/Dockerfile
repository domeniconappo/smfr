FROM efas/smfr_base:latest
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ARG http_proxy
ARG https_proxy

ENV http_proxy ${http_proxy}
ENV https_proxy ${http_proxy}

ENV INSTALL_PATH /geocoder
ENV LANG=C.UTF-8
ENV NUTS3_SHAPEFILE 2018_GlobalRegionsWGS84_CUT_WGS84Coord.shp
ENV LOGGING_LEVEL DEBUG

RUN mkdir -p $INSTALL_PATH && mkdir -p /scripts

RUN pip3.6 install spacy && python3.6 -m spacy download en_core_web_lg

COPY ./geocoder/requirements.txt ./
RUN pip3.6 install -r requirements.txt

WORKDIR $INSTALL_PATH
COPY ./geocoder/src/. ./
COPY ./geocoder/scripts/. /scripts/
COPY ./geocoder/config/. /config/

EXPOSE 5557

RUN chmod a+x /scripts/wait_for_it.sh
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandrasmfr:9160"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5557", "--workers", "1", "--log-level", "warning", "--timeout", "240", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "geotagger", "start:app"]
