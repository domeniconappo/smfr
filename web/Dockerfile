FROM python:3.6-alpine
MAINTAINER Domenico Nappo <domenico.nappo@ext.ec.europa.eu>
ENV INSTALL_PATH /web
RUN mkdir -p $INSTALL_PATH

WORKDIR $INSTALL_PATH

#COPY requirements.txt ./requirements.txt
ADD requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

#COPY ./src/. ./.
ADD ./src/. ./
EXPOSE 8888
