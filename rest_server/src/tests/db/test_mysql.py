from tests.db import DBTest

from smfrcore.models.sqlmodels import TwitterCollection


class TwitterCollectionTest(DBTest):

    def tearDown(self):
        self.addCleanup(TwitterCollection.query.delete)

    def test_empty(self):
        res = TwitterCollection.query
        self.assertEqual(res.count(), 0)

    def test_insert(self):
        session = self.db.session
        obj = TwitterCollection(collection_id=0, type='geo', trigger='background')
        session.add(obj)
        session.commit()
        res = TwitterCollection.query
        self.assertEqual(res.count(), 1)
        fetched = res[0]
        self.assertEqual(fetched.collection_id, 0)
        self.assertEqual(fetched.type, 'geo')
        self.assertEqual(fetched.trigger, 'background')

    def test_cleanup_insert(self):
        session = self.db.session
        obj = TwitterCollection(collection_id=0, type='geo', trigger='background')
        session.add(obj)
        session.commit()
        res = TwitterCollection.query
        self.assertEqual(res.count(), 1)
