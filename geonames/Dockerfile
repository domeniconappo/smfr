FROM docker.elastic.co/elasticsearch/elasticsearch:5.5.3
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV LANG=C.UTF-8
ENV INSTALL_PATH /geonames

WORKDIR $INSTALL_PATH

USER root

RUN mkdir -p /var/log/elasticsearch/
RUN chown -R elasticsearch /usr/share/elasticsearch/
RUN chown -R elasticsearch /var/log/elasticsearch/
RUN chown -R elasticsearch /usr/share/elasticsearch/data

USER elasticsearch
COPY geonames/elasticsearch.yml /usr/share/elasticsearch/config/
COPY geonames/log4j2.properties /usr/share/elasticsearch/config/
COPY geonames/jvm.options /usr/share/elasticsearch/config/
RUN elasticsearch-plugin remove x-pack

ENV PATH=$PATH:/usr/share/elasticsearch/bin
CMD ["elasticsearch"]

EXPOSE 9200 9300
