#!/usr/bin/env bash

source functions.sh

export FLASK_APP=smfr.py
export MYSQL_USER=$(getProperty "MYSQL_USER")
export MYSQL_PASSWORD=$(getProperty "MYSQL_PASSWORD")
export MYSQL_DBNAME=$(getProperty "MYSQL_DBNAME")
export CASSANDRA_KEYSPACE=$(getProperty "CASSANDRA_KEYSPACE")
export CASSANDRA_HOST=$(getProperty "CASSANDRA_HOST")
export CASSANDRA_NODES=$(getProperty "CASSANDRA_NODES")
export CASSANDRA_USE_CLUSTER=$(getProperty "CASSANDRA_USE_CLUSTER")
export CASSANDRA_PORT=$(getProperty "CASSANDRA_PORT")
export CASSANDRA_USER=$(getProperty "CASSANDRA_USER")
export CASSANDRA_PASSWORD=$(getProperty "CASSANDRA_PASSWORD")
export KAFKA_BOOTSTRAP_SERVER=$(getProperty "KAFKA_BOOTSTRAP_SERVER")

ipython
