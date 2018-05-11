FROM efas/smfr_base:latest
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ARG http_proxy
ARG https_proxy

ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}
ENV INSTALL_PATH /rest_server
ENV LANG=C.UTF-8


RUN apt-get install -y mariadb-client  && mkdir -p $INSTALL_PATH && mkdir -p /scripts && mkdir -p /configuration

COPY ./rest_server/requirements.txt $INSTALL_PATH/
RUN pip3.6 install -r $INSTALL_PATH/requirements.txt

RUN apt-get install -y mariadb-client && apt-get autoremove -y
ENV PATH=/usr/local/mysql/bin/:$PATH

COPY ./rest_server/src/. $INSTALL_PATH/
COPY ./rest_server/scripts/. /scripts/
COPY ./rest_server/src/config/. /configuration/

RUN ["chmod", "a+x", "/scripts/wait_for_it.sh"]

# Clean up APT
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR $INSTALL_PATH
EXPOSE 5555
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandrasmfr:9160"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5555", "--workers", "1", "--log-level", "warning", "--timeout", "240", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "restserver", "start:app"]
