from server.config import RestServerConfiguration
from start import app
from tests import SMFRTestCase


class DBTest(SMFRTestCase):

    config = RestServerConfiguration()

    @classmethod
    def setUpClass(cls):
        cls.app = app
        cls.client = app.test_client()
        cls.db = cls.config.db_mysql
        assert '_test' in str(cls.db.engine.url)
        cls.db.drop_all()
        cls.db.create_all()

    def setUp(self):
        self.db.create_all()
        self.db.session.commit()

    def tearDown(self):
        self.db.session.remove()
        self.db.drop_all()

