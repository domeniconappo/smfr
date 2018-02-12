CREATE KEYSPACE IF NOT EXISTS smfr_persistent WITH replication =
{'class':'SimpleStrategy','replication_factor':'1'};


CREATE TABLE IF NOT EXISTS smfr_persistent.tweet (
    collectionid int,
    ttype text,
    tweetid text,
    annotations map<timestamp, text>,
    nuts3 text,
    nuts3source text,
    timestamp timestamp,
    tweet text,
    PRIMARY KEY ((collectionid, ttype), tweetid)
) WITH CLUSTERING ORDER BY (tweetid ASC);