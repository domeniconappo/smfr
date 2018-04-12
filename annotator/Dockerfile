FROM python:3.6-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}
ENV INSTALL_PATH /annotator
ENV LANG=C.UTF-8

RUN mkdir -p $INSTALL_PATH && mkdir -p /scripts

WORKDIR $INSTALL_PATH
COPY ./annotator/requirements.txt ./
COPY ./annotator/src/. ./

RUN pip3.6 install --upgrade pip
RUN pip3.6 install -r requirements.txt

EXPOSE 5555
COPY ./annotator/scripts/. /scripts/

RUN ["chmod", "a+x", "/scripts/wait_for_it.sh"]
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandra:9042"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5555", "--workers", "1", "--log-level", "warning", "--timeout", "240", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "annotator", "start:app"]
