FROM efas/smfr_base:latest
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${http_proxy}
ENV INSTALL_PATH /annotator
ENV LANG=C.UTF-8

RUN mkdir -p $INSTALL_PATH && mkdir -p /scripts && mkdir -p /config

COPY ./annotator/requirements.txt $INSTALL_PATH/
RUN pip3.6 install -r $INSTALL_PATH/requirements.txt

COPY ./annotator/src/. $INSTALL_PATH/
COPY ./annotator/scripts/. /scripts/
COPY ./annotator/config/. /config/
WORKDIR $INSTALL_PATH
EXPOSE 5556

RUN ["chmod", "a+x", "/scripts/wait_for_it.sh"]
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandra:9042"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5556", "--workers", "1", "--log-level", "warning", "--timeout", "240", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "annotator", "start:app"]
