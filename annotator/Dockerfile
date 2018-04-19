FROM python:3.6
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${http_proxy}
ENV INSTALL_PATH /annotator
ENV LANG=C.UTF-8

ENV HDF5_MAJOR_REL       hdf5-1.10
ENV HDF5_MINOR_REL       hdf5-1.10.2
ENV HDF5_SRC_URL   http://www.hdfgroup.org/ftp/HDF5/releases
RUN cd /tmp                                                                                          ; \
    wget ${HDF5_SRC_URL}/${HDF5_MAJOR_REL}/${HDF5_MINOR_REL}/src/${HDF5_MINOR_REL}.tar.gz            ; \
    tar -xzvf ${HDF5_MINOR_REL}.tar.gz --directory /usr/local/src                                    ; \
    rm ${HDF5_MINOR_REL}.tar.gz                                                                      ; \
    cd /usr/local/src/${HDF5_MINOR_REL}                                                              ; \
    ./configure --prefix=/usr/local/hdf5                                                             ; \
    make                                                                                             ; \
    make check                                                                                       ; \
    make install                                                                                     ; \
    make check-install                                                                               ; \
    for f in /usr/local/hdf5/bin/* ; do ln -s $f /usr/local/bin ; done                               ; \
    pip3.6 install numpy                                                                             ; \
    cd /usr/local/src                                                                                ; \
    git clone https://github.com/h5py/h5py.git                                                       ; \
    cd h5py                                                                                          ; \
    export HDF5_DIR=/usr/local/hdf5                                                                  ; \
    python3.6 setup.py build                                                                         ; \
    python3.6 setup.py install

RUN pip3.6 install -U pip && pip3.6 install cassandra-driver flask

RUN mkdir -p $INSTALL_PATH && mkdir -p /scripts && mkdir -p /smfr_core && mkdir -p /config

COPY ./annotator/requirements.txt $INSTALL_PATH/
RUN pip3.6 install -r $INSTALL_PATH/requirements.txt

COPY ./smfr_core/. /smfr_core/
WORKDIR /smfr_core
RUN python3.6 setup.py build && python3.6 setup.py install

COPY ./annotator/src/. $INSTALL_PATH/
COPY ./annotator/scripts/. /scripts/

COPY ./annotator/config/. /config/
WORKDIR $INSTALL_PATH
EXPOSE 5556

RUN ["chmod", "a+x", "/scripts/wait_for_it.sh"]
ENTRYPOINT ["/scripts/wait_for_it.sh", "cassandra:9042"]
CMD ["--", "gunicorn", "-b", "0.0.0.0:5556", "--workers", "1", "--log-level", "warning", "--timeout", "240", "--threads", "4", "--worker-class", "gthread", "--reload", "--access-logfile", "-", "--name", "annotator", "start:app"]
