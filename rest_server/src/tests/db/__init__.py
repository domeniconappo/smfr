from server.config import server_configuration
from start import app
from tests import SMFRTestCase


class DBTest(SMFRTestCase):

    config = server_configuration()

    @classmethod
    def setUpClass(cls):

        cls.app = app.test_client()
        cls.db = cls.config.db_mysql
        cls.db.drop_all()
        cls.db.create_all()
