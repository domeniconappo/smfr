from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from server.config import server_configuration

configuration = server_configuration()


def cassandra_session_factory():
    hosts = configuration.flask_app.config['CASSANDRA_HOSTS']
    keyspace = configuration.flask_app.config['CASSANDRA_KEYSPACE']
    cluster = Cluster(hosts)
    session = cluster.connect()
    session.row_factory = dict_factory
    session.execute("USE {}".format(keyspace))
    return session

