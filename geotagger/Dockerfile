FROM python:3.6
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${http_proxy}

ENV INSTALL_PATH /geotagger
ENV LANG=C.UTF-8
ENV NUTS3_SHAPEFILE 2018_GlobalRegionsWGS84_CUT_WGS84Coord.shp
ENV LOGGING_LEVEL DEBUG

RUN pip3.6 install -U pip && pip3.6 install cassandra-driver flask

RUN mkdir -p $INSTALL_PATH && mkdir -p /scripts && mkdir -p /smfr_core

RUN pip3.6 install spacy && python3.6 -m spacy download en_core_web_lg

COPY ./geotagger/requirements.txt ./
RUN pip3.6 install -r requirements.txt

COPY ./smfr_core/. /smfr_core/
WORKDIR /smfr_core
RUN python3.6 setup.py build && python3.6 setup.py install


WORKDIR $INSTALL_PATH
COPY ./geotagger/src/. ./
COPY ./geotagger/scripts/. /scripts/
COPY ./geotagger/config/. /config/

EXPOSE 5557

RUN chmod a+x /scripts/wait_for_it.sh
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandra:9042"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5557", "--workers", "1", "--log-level", "warning", "--timeout", "240", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "geotagger", "start:app"]
