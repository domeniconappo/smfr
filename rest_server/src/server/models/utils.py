from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from server.config import RestServerConfiguration


def cassandra_session_factory():
    configuration = RestServerConfiguration()
    hosts = configuration.flask_app.config['CASSANDRA_HOSTS']
    keyspace = configuration.flask_app.config['CASSANDRA_KEYSPACE']
    cluster = Cluster(hosts)
    session = cluster.connect()
    session.row_factory = dict_factory
    session.execute("USE {}".format(keyspace))
    return session

