#!/usr/bin/python
# -*- coding: utf-8 -*-

# import unittest
# import json
import sys

from smfrcore.models.sql import TwitterCollection, create_app
from smfrcore.models.cassandra import Tweet

from tests.tweets_data import data as tweets


# class ReconcileTestCase(unittest.TestCase):
# tweets_to_test = [1039905041103171584]
tweets_to_test = []


def test_reconcile():
    keywords = ['Stepanakert', 'Svolvaer', 'Nordland', 'Steinkjer', 'Trondheim', 'Sor-Trondelag', 'Rorvik', 'Huesca', 'Sr-Trndelag',
                'Tovuz', 'Lillehammer', 'Buynaksk', 'Ali Bayramli', 'Mo i Rana',
                'Kaspiysk', 'Agdam', 'Samegrelo-Zemo Svaneti', 'Gjovik', 'Balakan', 'Nord-Trondelag', 'Derbent', 'Nord-Trndelag',
                'Yevlax', 'Goycay', 'Azerbaijan', 'Qabala', 'Oguz', 'Respublika Dagestan', 'Oppland', 'Saki', 'Hamar', 'Ganca',
                'Makhachkala', 'Zaqatala', 'Lleida', 'Goranboy', 'Hedmark', 'Namsos', 'Gadabay', 'Narvik', 'Guria', 'Kakheti']
    bboxes = ['45.09,38.767,49.534,41.842', '46.179,41.567,46.749,41.902', '41.712,41.786,42.666,42.171', '45.023,41.054,46.737,42.55',
              '45.104,41.187,48.594,44.998', '41.722,42.076,41.92,42.212', '46.297,41.377,47.024,41.863', '-0.933,41.348,0.768,42.922',
              '0.32,41.276,1.855,42.861', '9.584,59.841,12.871,62.697',
              '8.281,62.256,12.255,64.429', '10.135,63.181,14.326,65.149', '11.706,64.939,18.151,69.315', '7.343,60.132,11.154,62.378']
    app = create_app()
    app.app_context().push()

    collections = TwitterCollection.query.filter_by(trigger=TwitterCollection.TRIGGER_ONDEMAND).all()
    numnotreconciled = 0
    numreconciledbbox = 0
    numreconciledkw = 0
    for t in tweets:
        if tweets_to_test and t['id'] not in tweets_to_test:
            continue
        reconciled = False
        for c in collections:
            if c.is_tweet_in_bounding_box(t):
                # print(' >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> BBOX Reconciled: %d' % t['id'])
                reconciled = True
                numreconciledbbox += 1
                # print(c.locations, Tweet.coords_from_raw_tweet(t), Tweet.get_tweet_bbox(t))
                # print(c.tracking_keywords, Tweet.full_text_from_raw_tweet(t))
            elif c.tweet_matched_keyword(t):
                # print(' >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> KW Reconciled: %d' % t['id'])
                reconciled = True
                numreconciledkw += 1
                # print(c.tracking_keywords, Tweet.full_text_from_raw_tweet(t))
                # print(c.locations, Tweet.coords_from_raw_tweet(t), Tweet.get_tweet_bbox(t))
            if reconciled:
                break
        if not reconciled:
            numnotreconciled += 1
            print(' XXXXXXXXXXXXXXXXXXXXXXXXXX NOT Reconciled: %d' % t['id'])
            # print(Tweet.coords_from_raw_tweet(t), Tweet.get_tweet_bbox(t))
            print('\n================================= START MATCHING TEXT =====================================\n',
                  Tweet.get_contributing_match_keywords_fields(t))
            print('\n================================= END MATCHING TEXT =====================================\n')

    print('Reconciled bbox: %d' % numreconciledbbox)
    print('Reconciled keywords: %d' % numreconciledkw)
    print('NOT reconciled: %d' % numnotreconciled)


if __name__ == '__main__':
    sys.exit(test_reconcile())
