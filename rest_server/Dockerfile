FROM ubuntu:16.04
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}
ENV INSTALL_PATH /rest_server
ENV LANG=C.UTF-8

RUN apt-get update
RUN apt-get install -y software-properties-common apt-file apt-utils python-software-properties &&                              \
    add-apt-repository ppa:deadsnakes/ppa &&                                                                                    \
    add-apt-repository 'deb [arch=amd64,i386] http://www.ftp.saix.net/DB/mariadb/repo/10.3/ubuntu xenial main' &&               \
    apt-key adv --keyserver-options "http-proxy=$http_proxy" --keyserver hkp://keyserver.ubuntu.com:80 --recv F1656F24C74CD1D8

RUN apt-get update
RUN apt-get install -y zlib1g-dev libmariadbclient-dev libmariadbd-dev python3.6 python3.6-dev                                  \
    liblapack-dev liblapack-doc-man liblapack-doc liblapack-pic liblapack3 liblapack-test                                       \
    liblapacke liblapacke-dev hdf5-tools libhdf5-serial-dev gfortran g++ wget                                                   \
    libssl-dev openssl &&                                                                                                       \
    curl https://bootstrap.pypa.io/get-pip.py | python3.6  &&                                                                   \
    mkdir -p $INSTALL_PATH && mkdir -p $INSTALL_PATH/client && chmod a+w -R $INSTALL_PATH/client && mkdir -p /scripts &&        \
    mkdir -p /configuration && mkdir -p /smfr_core

WORKDIR $INSTALL_PATH

RUN pip3.6 install --upgrade pip
RUN pip3.6 install cassandra-driver https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-1.6.0-cp36-cp36m-linux_x86_64.whl && \
    ln -s /usr/bin/mariadb_config /usr/bin/mysql_config &&                                                                              \
    pip3.6 install spacy &&                                                                                                             \
    python3.6 -m spacy download en_core_web_lg

COPY ./rest_server/requirements.txt ./
RUN pip3.6 install -r requirements.txt

RUN apt-get remove -y libmariadbclient-dev libmariadbd-dev && apt-get install -y mariadb-client
RUN apt-get autoremove -y
ENV PATH=/usr/local/mysql/bin/:$PATH

COPY ./smfr_core/. /smfr_core/
WORKDIR /smfr_core
RUN python3.6 setup.py install

COPY ./rest_server/src/. ./
COPY ./rest_server/scripts/. /scripts/
COPY ./rest_server/src/config/. /configuration/

RUN ["chmod", "a+x", "/scripts/wait_for_it.sh"]

WORKDIR $INSTALL_PATH
EXPOSE 5555
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandra:9042"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5555", "--workers", "1", "--log-level", "warning", "--timeout", "240", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "restserver", "start:app"]