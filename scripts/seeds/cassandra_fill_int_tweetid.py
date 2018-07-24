from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, default_lbp_factory


def read_env():
    split_properties = [line.split("=") for line in open('.env') if line and line != '\n' and not line.startswith('#')]
    props = {key: value.strip() for key, value in split_properties}
    return props


if __name__ == '__main__':
    properties = read_env()

    cassandra_user = properties.get('CASSANDRA_USER')
    cassandra_password = properties.get('CASSANDRA_PASSWORD')

    cluster = Cluster(auth_provider=PlainTextAuthProvider(username=cassandra_user, password=cassandra_password),
                      load_balancing_policy=default_lbp_factory())

    session = cluster.connect('smfr_persistent')

    rows = session.execute('SELECT collectionid, tweetid, ttype FROM tweet')

    for i, row in enumerate(rows, start=1):
        session.execute(
            'UPDATE tweet SET tweet_id=%s WHERE collectionid=%s AND tweetid=%s AND ttype=%s',
            (int(row.tweetid), row.collectionid, row.tweetid, row.ttype)
        )
        if not (i % 5000):
            print(i)
