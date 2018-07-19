FROM cassandra:3.11.2

ENV LOCAL_JMX no

COPY cassandrasmfr/jmxremote.password /etc/cassandra/jmxremote.password
COPY cassandrasmfr/jmxremote.access /etc/cassandra/jmxremote.access
COPY cassandrasmfr/cassandra.yaml /etc/cassandra/cassandra.yaml

RUN chown cassandra:cassandra /etc/cassandra/jmxremote.*
RUN chmod 400 /etc/cassandra/jmxremote.*
