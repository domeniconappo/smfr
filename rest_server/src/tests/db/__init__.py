from server.config import RestServerConfiguration
from start import app
from tests import SMFRTestCase


class DBTest(SMFRTestCase):

    config = RestServerConfiguration()

    @classmethod
    def setUpClass(cls):

        cls.app = app.test_client()
        cls.db = cls.config.db_mysql
        assert '_test' in cls.db.engine.url
        cls.db.drop_all()
        cls.db.create_all()
