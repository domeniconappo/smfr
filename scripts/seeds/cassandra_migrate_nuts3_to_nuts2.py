from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('smfr_persistent')

rows = session.execute('SELECT collectionid, tweetid, nuts3, nuts3source, ttype FROM tweet')

for i, row in enumerate(rows, start=1):
    session.execute(
        'UPDATE tweet SET nuts2=%s, nuts2source=%s WHERE collectionid=%s AND tweetid=%s AND ttype=%s',
        (row.nuts3, row.nuts3source, row.collectionid, row.tweetid, row.ttype)
    )
    if not (i % 1000):
        print(i)
