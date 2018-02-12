from tests.db import DBTest

from server.models import VirtualTwitterCollection


class VirtualTwitterCollectionTest(DBTest):

    def tearDown(self):
        self.addCleanup(VirtualTwitterCollection.query.delete)

    def test_empty(self):
        res = VirtualTwitterCollection.query
        self.assertEqual(res.count(), 0)

    def test_insert(self):
        session = self.db.session
        obj = VirtualTwitterCollection(collection_id=0, type='geo', trigger='background')
        session.add(obj)
        session.commit()
        res = VirtualTwitterCollection.query
        self.assertEqual(res.count(), 1)
        fetched = res[0]
        self.assertEqual(fetched.collection_id, 0)
        self.assertEqual(fetched.type, 'geo')
        self.assertEqual(fetched.trigger, 'background')

    def test_cleanup_insert(self):
        session = self.db.session
        obj = VirtualTwitterCollection(collection_id=0, type='geo', trigger='background')
        session.add(obj)
        session.commit()
        res = VirtualTwitterCollection.query
        self.assertEqual(res.count(), 1)
