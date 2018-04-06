FROM elasticsearch:5.5.2
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}
ENV INSTALL_PATH /geonames

WORKDIR $INSTALL_PATH

# this must be done on host and move the extracted folders to the volume folder
# that is mapped to /var/lib/elasticsearch/elasticsearch/, as configured in docker-compose.yaml
# (e.g. /DATA/smfr/data/geonames_index)
#RUN wget https://s3.amazonaws.com/ahalterman-geo/geonames_index.tar.gz
#RUN tar -xzf geonames_index.tar.gz
#
#RUN mv $INSTALL_PATH/geonames_index /var/lib/elasticsearch/elasticsearch/
#RUN rm geonames_index.tar.gz

RUN chmod -R a+w /var/lib/elasticsearch/
RUN chmod -R g+w /var/lib/elasticsearch/
COPY geonames/elasticsearch.yml /usr/share/elasticsearch/config/
COPY geonames/log4j2.properties /usr/share/elasticsearch/config/

ENV PATH=$PATH:/usr/share/elasticsearch/bin
USER elasticsearch
CMD ["elasticsearch"]

EXPOSE 9200 9300
