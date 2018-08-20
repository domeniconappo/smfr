
CREATE TABLE smfr_persistent.tweet (
    collectionid int,
    ttype text,
    tweetid text,
    annotations map<text, frozen<tuple<text, decimal>>>,
    created_at timestamp,
    geo map<text, text>,
    lang text,
    latlong frozen<tuple<decimal, decimal>>,
    nuts2 text,
    nuts2source text,
    tweet text,
    tweet_id bigint,
    PRIMARY KEY ((collectionid, ttype), tweetid)
) WITH CLUSTERING ORDER BY (tweetid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': '99'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX tweet_lang_idx ON smfr_persistent.tweet (lang);
CREATE INDEX tweet_tweet_id_idx ON smfr_persistent.tweet (tweet_id);
CREATE INDEX tweet_created_at_idx ON smfr_persistent.tweet (created_at);
CREATE INDEX tweet_collectionid_idx ON smfr_persistent.tweet (collectionid);
