FROM elasticsearch:5.5.2
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>

ENV http_proxy ${http_proxy}
ENV https_proxy ${https_proxy}
ENV INSTALL_PATH /geonames

WORKDIR $INSTALL_PATH

RUN wget https://s3.amazonaws.com/ahalterman-geo/geonames_index.tar.gz
RUN tar -xzf geonames_index.tar.gz
RUN mv $(pwd)/geonames_index/* /usr/share/elasticsearch/data/
RUN chown -R elasticsearch. /usr/share/elasticsearch/
RUN chmod -R a+w /usr/share/elasticsearch/

# Mount elasticsearch.yml config
#COPY geonames/elasticsearch.yaml /usr/share/elasticsearch/config/elasticsearch.yml

EXPOSE 9200
EXPOSE 9300
