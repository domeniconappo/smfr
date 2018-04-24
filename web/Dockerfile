FROM efas/smfr_base:latest
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>
ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}

ENV INSTALL_PATH /web
RUN mkdir -p $INSTALL_PATH

WORKDIR $INSTALL_PATH

COPY ./web/requirements.txt ./
RUN pip install -U pip && pip install -r requirements.txt

COPY ./web/src/. ./
COPY ./web/scripts/. /scripts/
RUN chmod a+x -R /scripts/

EXPOSE 8888
CMD ["/scripts/start.sh"]
