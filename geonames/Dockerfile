FROM elasticsearch:5.5.2
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV LANG=C.UTF-8
ENV INSTALL_PATH /geonames

WORKDIR $INSTALL_PATH

RUN chown -R elasticsearch /var/lib/elasticsearch/
RUN chown -R elasticsearch /var/log/elasticsearch/

COPY geonames/elasticsearch.yml /usr/share/elasticsearch/config/
COPY geonames/log4j2.properties /usr/share/elasticsearch/config/
COPY geonames/jvm.options /usr/share/elasticsearch/config/

ENV PATH=$PATH:/usr/share/elasticsearch/bin
USER elasticsearch
CMD ["elasticsearch"]

EXPOSE 9200 9300
