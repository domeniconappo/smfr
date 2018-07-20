FROM cassandra:3.11.2

ENV LOCAL_JMX no

COPY cassandrasmfr/jmxremote.password /etc/cassandra/jmxremote.password
COPY cassandrasmfr/jmxremote.access /etc/cassandra/jmxremote.access
RUN chown cassandra:cassandra /etc/cassandra/jmxremote.*
COPY cassandrasmfr/cassandra.yaml /etc/cassandra/cassandra.yaml
RUN chmod 400 /etc/cassandra/jmxremote.*
#RUN cqlsh -u cassandra -p cassandra --execute="CREATE USER ${CASSANDRA_USER} WITH PASSWORD ${CASSANDRA_PASSWORD} SUPERUSER;"
#RUN cqlsh -u cassandra -p cassandra --execute="ALTER USER cassandra WITH PASSWORD ${CASSANDRA_PASSWORD};"
